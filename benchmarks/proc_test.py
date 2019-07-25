from . import proc
import paramiko
import unittest


class ParamikoProcTest(unittest.TestCase):
    def _client(self) -> paramiko.SSHClient:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy)
        client.connect('localhost')
        return client

    def test_wait(self):
        p = proc.ParamikoProc(client = self._client(),
                              args = 'true',
                              stdout = '/tmp/out.txt',
                              stderr = '/tmp/err.txt')
        self.assertEqual(p.wait(), 0)

    # Smoke test.
    def test_kill(self):
        p = proc.ParamikoProc(client = self._client(),
                              args = ['sleep', '1000'],
                              stdout = '/tmp/out.txt',
                              stderr = '/tmp/err.txt')
        p.kill()

    # Smoke test.
    def test_double_kill(self):
        p = proc.ParamikoProc(client = self._client(),
                              args = ['sleep', '1000'],
                              stdout = '/tmp/out.txt',
                              stderr = '/tmp/err.txt')
        p.kill()
        p.kill()

    # Smoke test.
    def test_pid(self):
        p = proc.ParamikoProc(client = self._client(),
                              args = ['sleep', '1000'],
                              stdout = '/tmp/out.txt',
                              stderr = '/tmp/err.txt')
        p.pid()
        p.kill()


if __name__ == '__main__':
    unittest.main()
