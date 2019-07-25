from typing import Optional, Sequence, Union
import abc
import mininet
import mininet.node
import paramiko
import random
import string
import subprocess
import time


def _random_string(n: int) -> str:
    return ''.join(random.choice(string.ascii_uppercase) for _ in range(n))


def _canonicalize_args(args: Union[str, Sequence[str]]) -> str:
    if isinstance(args, str):
        return args
    else:
        return subprocess.list2cmdline(args)


# A Proc represents a process running on some machine. A Proc is like a
# subprocess.Popen (with fewer methods) that is able to run either locally or
# on a remote machine.
#
# You construct a Proc with a command represented either as a string or as a
# list of strings, just like you do with subprocess.Proc. You also provide
# filenames into which the stdout and stderr of the command are written. For
# example, the following two commands are identical. They both run "ls -l"
# locally, writing stdout and stderr to /tmp/o.txt and /tmp/e.txt respectively.
#
# >>> proc = PopenProc('ls -l', stdout='/tmp/o.txt', stderr='/tmp/e.txt')
# >>> proc = PopenProc(['ls', '-l'], stdout='/tmp/o.txt', stderr='/tmp/e.txt')
#
# Note that the filenames into which stdout and stderr are written are relative
# to the machine on which the process runs. In the example above, the process
# was run locally, so the results are written locally. However, if you run a
# remote process, the results are written on the machine where the command
# runs. For example, the commands below execute `hostname` on the machine at
# adress 1.2.3.4. The results are written to /tmp/out.txt and /tmp/err.txt on
# machine 1.2.3.4, not locally.
#
# >>> client = paramiko.SSHClient()
# >>> client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy)
# >>> client.connect('1.2.3.4')
# >>> proc = ParamikoProc('hostname', '/tmp/out.txt', '/tmp/err.txt')
class Proc(abc.ABC):
    @abc.abstractmethod
    def cmd(self) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def pid(self) -> Optional[int]:
        raise NotImplementedError()

    @abc.abstractmethod
    def wait(self) -> Optional[int]:
        raise NotImplementedError()

    @abc.abstractmethod
    def kill(self) -> None:
        raise NotImplementedError()


# A PopenProc is just a wrapper around a locally run subprocess.Popen.
class PopenProc(Proc):
    def __init__(self,
                 args: Union[str, Sequence[str]],
                 stdout: str,
                 stderr: str) -> None:
        self._cmd = _canonicalize_args(args)
        self._popen = subprocess.Popen(args,
                                       stdout=open(stdout, 'w'),
                                       stderr=open(stderr, 'w'))

    def cmd(self) -> str:
        return self._cmd

    def pid(self) -> Optional[int]:
        return self._popen.pid

    def wait(self) -> Optional[int]:
        self._popen.wait()
        return self._popen.returncode

    def kill(self) -> None:
        self._popen.kill()


# A ParamikoProc is a process run on a remote machine over SSH via paramiko.
# Paramiko makes it easy to run commands on another machine. You simply get a
# hold of a channel and run `channel.exec_command`. However, paramiko does not
# make it easy to _kill_ a command that is currently being run via
# exec_command.
#
# There are a couple of different possible solutions [1], but none are great.
# For example, if you call `channel.get_pty` before `channel.exec_command`,
# then calling `channel.close` will send a SIGHUP to the command and kill it
# (normally). However, if you call `channel.get_pty` too many times, paramiko
# crashes. I'm not exactly sure why. Moreover, the paramiko documentation
# suggests not calling `get_pty` before issuing `exec_command` [2].
#
# We implement the following solution. It's not great, but it seems to work ok.
# First, every ParamikoProc generates a unique nonce. When we call
# `channel.exec_command`, we echo the nonce before the command, like this:
# `echo <nonce>; <cmd>`. When we do this, calling `pgrep -f <nonce>` returns
# the pid of a command that looks like this: `bash -c echo <nonce>; <cmd>`.
# This command is the parent of `<cmd>`. We then use `pgrep -P` to get the pid
# of the subprocess `<cmd>`. We then kill the process using this pid.
#
# [1]: https://stackoverflow.com/q/7734679/3187068
# [2]: http://docs.paramiko.org/en/latest/api/channel.html#paramiko.channel.Channel.get_pty
class ParamikoProc(Proc):
    def __init__(self,
                 client: paramiko.SSHClient,
                 args: Union[str, Sequence[str]],
                 stdout: str,
                 stderr: str) -> None:
        self._nonce = _random_string(80)
        self._cmd = (f'echo {self._nonce}; ' +
                     f'({_canonicalize_args(args)}) 2> "{stderr}" > "{stdout}"')
        self._client = client
        self._channel = client.get_transport().open_session()
        self._channel.exec_command(self._cmd)
        self._pgid: Optional[int] = None
        self._pid: Optional[int] = None
        self._killed: bool = False

    def _get_pgid(self) -> Optional[int]:
        while True:
            # If the channel is already finished, then it's too late for us to
            # get a pid.
            if self._channel.exit_status_ready():
                return None
            try:
                _, out, _ = self._client.exec_command(f'pgrep -f {self._nonce}')
                out.channel.recv_exit_status()
                return int(out.read().decode("utf-8").strip())
            except ValueError:
                pass

    def _get_pid(self) -> Optional[int]:
        while True:
            # If the channel is already finished, then it's too late for us to
            # get a pid.
            if self._channel.exit_status_ready():
                return None

            # If the pgid is None, we can't get the pid.
            if self.pgid() is None:
                return None

            try:
                _, out, _ = self._client.exec_command(f'pgrep -P {self.pgid()}')
                out.channel.recv_exit_status()
                return int(out.read().decode("utf-8").strip())
            except ValueError:
                pass

    def cmd(self) -> str:
        return self._cmd

    def pgid(self) -> Optional[int]:
        if self._pgid is not None:
            return self._pgid
        else:
            self._pgid = self._get_pgid()
            return self._pgid

    def pid(self) -> Optional[int]:
        if self._pid is not None:
            return self._pid
        else:
            self._pid = self._get_pid()
            return self._pid

    def wait(self) -> Optional[int]:
        self.returncode = self._channel.recv_exit_status()
        return self.returncode

    def kill(self) -> None:
        # If we've already killed the process, we don't have to kill it again.
        if self._killed:
            return

        # If the process is already dead, we don't have to kill it. Otherwise,
        # we do.
        if self._channel.exit_status_ready():
            pass
        else:
            pgid = self.pgid()
            if pgid:
                _, out, _ = self._client.exec_command(f'sudo kill -- -{pgid}')
                out.channel.recv_exit_status()

        self._channel.close()
        self._killed = True


class MininetProc(Proc):
    def __init__(self,
                 node: mininet.node.Node,
                 args: Union[str, Sequence[str]],
                 stdout: str,
                 stderr: str) -> None:
        self._node = node
        self._cmd = _canonicalize_args(args)
        self._popen = node.popen(args,
                                 stdout=open(stdout, 'w'),
                                 stderr=open(stderr, 'w'))

    def cmd(self) -> str:
        return self._cmd

    def pid(self) -> Optional[int]:
        return self._popen.pid

    def wait(self) -> Optional[int]:
        return self._popen.wait()

    def kill(self) -> None:
        self._popen.kill()
