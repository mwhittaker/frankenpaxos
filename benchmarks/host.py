from . import proc
from typing import NamedTuple, Sequence, Union
import mininet
import mininet.node
import paramiko

# A Host represents a machine (potentially virtual) on which you can run
# processes. A Host may represent the local machine (LocalHost), a remote
# machine (RemoteHost), or a virtual machine in Mininet (MininetHost).
class Host:
    def ip(self) -> str:
        raise NotImplementedError()

    def popen(self,
              args: Union[str, Sequence[str]],
              stdout: str,
              stderr: str) -> proc.Proc:
        raise NotImplementedError()


# An endpoint is a host and port. Typically, you launch a server that listens
# at a particular endpoint.
class Endpoint(NamedTuple):
    host: Host
    port: int


class LocalHost(Host):
    def ip(self) -> str:
        return "127.0.0.1"

    def popen(self,
              args: Union[str, Sequence[str]],
              stdout: str,
              stderr: str) -> proc.Proc:
        return proc.PopenProc(args, stdout=stdout, stderr=stderr)


class RemoteHost(Host):
    def __init__(self, client: paramiko.SSHClient) -> None:
        self.client = client

    def ip(self) -> str:
        (ip, _port) = self.client.get_transport().getpeername()
        return ip

    def popen(self,
              args: Union[str, Sequence[str]],
              stdout: str,
              stderr: str) -> proc.Proc:
        return proc.ParamikoProc(self.client, args, stdout=stdout,
                                 stderr=stderr)


class MininetHost(Host):
    def __init__(self, node: mininet.node.Node) -> None:
        self.node = node

    def ip(self) -> str:
        return self.node.IP()

    def popen(self,
              args: Union[str, Sequence[str]],
              stdout: str,
              stderr: str) -> proc.Proc:
        return proc.MininetProc(self.node, args, stdout=stdout, stderr=stderr)
