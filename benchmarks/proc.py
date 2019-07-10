from typing import Optional, Sequence, Union
import mininet
import mininet.node
import paramiko
import subprocess


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
class Proc:
    def __init__(self,
                 args: Union[str, Sequence[str]],
                 stdout: str,
                 stderr: str) -> None:
        if isinstance(args, str):
            self.cmd = args
        else:
            self.cmd = subprocess.list2cmdline(args)
        self.args = args
        self.stdout = stdout
        self.stderr = stderr
        self.returncode: Optional[int] = None

    def get_cmd(self) -> str:
        return self.cmd

    def wait(self) -> Optional[int]:
        raise NotImplementedError()

    def kill(self) -> None:
        raise NotImplementedError()


class PopenProc(Proc):
    def __init__(self,
                 args: Union[str, Sequence[str]],
                 stdout: str,
                 stderr: str) -> None:
        super().__init__(args, stdout, stderr)
        self.popen = subprocess.Popen(args,
                                      stdout=open(stdout, 'w'),
                                      stderr=open(stderr, 'w'))
        self.returncode = None

    def wait(self) -> Optional[int]:
        self.popen.wait()
        self.returncode = self.popen.returncode
        return self.returncode

    def kill(self) -> None:
        self.popen.kill()
        self.returncode = self.popen.returncode


class ParamikoProc(Proc):
    def __init__(self,
                 client: paramiko.SSHClient,
                 args: Union[str, Sequence[str]],
                 stdout: str,
                 stderr: str) -> None:
        super().__init__(args, stdout, stderr)
        self.cmd = f'{self.cmd} 2> "{stderr}" > "{stdout}"'
        self.client = client
        self.channel = client.get_transport().open_session()
        # By getting a PTY, when the channel is closed, the command we're
        # running will be sent a SIGHUP and die. I don't fully understand the
        # details behind all this, but it seems to work ok.
        self.channel.get_pty()
        self.channel.set_environment_variable(name='foo', value='bar')
        self.channel.exec_command(self.cmd)

    def get_cmd(self) -> str:
        return self.cmd

    def wait(self) -> Optional[int]:
        self.returncode = self.channel.recv_exit_status()
        return self.returncode

    def kill(self) -> None:
        self.channel.close()
        self.returncode = self.channel.recv_exit_status()


class MininetProc(Proc):
    def __init__(self,
                 node: mininet.node.Node,
                 args: Union[str, Sequence[str]],
                 stdout: str,
                 stderr: str) -> None:
        super().__init__(args, stdout, stderr)
        self.popen = node.popen(args,
                                stdout=open(stdout, 'w'),
                                stderr=open(stderr, 'w'))
        self.returncode = None

    def wait(self) -> Optional[int]:
        self.popen.wait()
        self.returncode = self.popen.returncode
        return self.returncode

    def kill(self) -> None:
        self.popen.kill()
        self.returncode = self.popen.returncode
