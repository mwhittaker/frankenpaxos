from . import benchmark
from . import host
from . import proc
from typing import Optional


class JavaPerfProc:
    def __init__(self,
                 bench: benchmark.BenchmarkDirectory,
                 host: host.Host,
                 proc: proc.Proc,
                 label: str) -> None:
        self.bench = bench
        self.host = host
        self.proc = proc
        self.label = label

        # If the process has finished, we have no hope of profiling it.
        pid = proc.pid()
        if pid is None:
            self.perf_record = None
        else:
            self.perf_record = bench.popen(
                host = host,
                label = f'{label}_perf_record',
                cmd =  [
                    'sudo',
                    'perf',
                    'record',
                    '-o', bench.abspath(f'perf-{pid}.data'),
                    '-p', str(proc.pid()),
                    '--timestamp',
                    '-g',
                    '--', 'sleep', '1000000000000000000000'
                ]
            )

    def get_cmd(self) -> str:
        return self.proc.get_cmd()

    def pid(self) -> Optional[int]:
        return self.proc.pid()

    def wait(self) -> Optional[int]:
        return self.proc.wait()

    def kill(self) -> None:
        if self.perf_record is None:
            self.proc.kill()
            return

        self.perf_record.kill()
        self.bench.popen(
            host = self.host,
            label = f'{self.label}_create_java_perf_map',
            cmd =  ['create-java-perf-map.sh', str(self.proc.pid())]
        ).wait()
        self.bench.popen(
            host = self.host,
            label = f'{self.label}_create_java_perf_map_mv',
            cmd = [
                'sudo',
                'mv',
                f'/tmp/perf-{self.proc.pid()}.map',
                self.bench.abspath(f'perf-{self.proc.pid()}.map')
            ]
        ).wait()
        self.proc.kill()
