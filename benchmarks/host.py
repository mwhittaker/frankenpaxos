from . import proc
from typing import Sequence, Union
import mininet
import paramiko


class Host:
    def popen(self,
              args: Union[str, Sequence[str]],
              stdout: str,
              stderr: str) -> proc.Proc:
        raise NotImplementedError()


class LocalHost(Host):
    def popen(self,
              args: Union[str, Sequence[str]],
              stdout: str,
              stderr: str) -> proc.Proc:
        return proc.PopenProc(args, stdout=stdout, stderr=stderr)


class RemoteHost(Host):
    def __init__(self, client: paramiko.SSHClient) -> None:
        self.client = client

    def popen(self,
              args: Union[str, Sequence[str]],
              stdout: str,
              stderr: str) -> proc.Proc:
        return proc.ParamikoProc(self.client, args, stdout=stdout,
                                 stderr=stderr)


class MininetHost(Host):
    def __init__(self, node: mininet.node.Node) -> None:
        self.node = node

    def popen(self,
              args: Union[str, Sequence[str]],
              stdout: str,
              stderr: str) -> proc.Proc:
        return proc.MininetProc(self.node, args, stdout=stdout, stderr=stderr)
