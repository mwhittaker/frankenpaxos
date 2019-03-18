import contextlib
import pandas as pd
import subprocess

# Imagine you have the following python code in a file called sleep.py. The
# code creates a subprocess and waits for it to terminate.
#
#   p = subprocess.Popen(['sleep', '100'])
#   p.wait()
#
# If you run `python sleep.py` and then kill the process before it terminates,
# sometimes the subprocess lives on. The Reaped context manager ensures that
# the process is killed, even if an exception is thrown.
class Reaped(object):
    def __init__(self, p: subprocess.Popen) -> None:
        self.p = p

    def __enter__(self) -> subprocess.Popen:
        return self.p

    def __exit__(self, cls, exn, traceback) -> None:
        self.p.terminate()
