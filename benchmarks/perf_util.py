from . import benchmark
from . import host
from . import proc
from typing import Optional


# Profiling a Java program using perf is a little bit involved [1]. Roughly,
# you have to follow the following steps:
#
#   1. Start the Java program and note the pid p.
#   2. Run `perf record` on the pid.
#   3. Before the Java program terminates, stop running `perf record`.
#   4. Before the Java program terminates, run `create-java-perf-map.sh` [2].
#   5. Run `perf script` (and probably other stuff to make a flamegraph [3]).
#
# JavaPerfProc helps automate these steps. A JavaPerfProc is instantiated with
# a proc. It immediately runs `perf record` on the process (step 2). When the
# JavaPerfProc is killed, it stops running perf record and runs
# `create-java-perf-map.sh` (steps 3 and 4). Because steps 3 and 4 must be run
# before the underlying Java process is killed, you must kill the JavaPerfProc
# before you kill the underlying proc. To make this easier to do, a
# JavaPerfProc masquerades as the proc it is instantiated with. Thus, if you
# have code like this:
#
#     proc = get_proc()
#
# you can change it to this:
#
#     proc = get_proc()
#     proc = JavaPerfProc(bench, host, proc, label)
#
# and the rest of your code should "just work".
#
# [1]: https://medium.com/netflix-techblog/java-in-flames-e763b3d32166
# [2]: https://github.com/jvm-profiling-tools/perf-map-agent
# [3]: https://github.com/brendangregg/FlameGraph
class JavaPerfProc(proc.Proc):
    def __init__(self, bench: benchmark.BenchmarkDirectory, host: host.Host,
                 proc: proc.Proc, label: str) -> None:
        self._bench = bench
        self._host = host
        self._proc = proc
        self._label = label
        self._killed: bool = False

        # If the process has finished, we have no hope of profiling it.
        pid = proc.pid()
        if pid is None:
            self._perf_record = None
        else:
            self._perf_record = bench.popen(
                host=host,
                label=f'{label}_perf_record',
                cmd=[
                    'sudo', 'perf', 'record', '-o',
                    bench.abspath(f'perf-{pid}.data'), '-p',
                    str(proc.pid()), '--timestamp', '-g', '--', 'sleep',
                    '1000000000000000000000'
                ])

    def cmd(self) -> str:
        return self._proc.cmd()

    def pid(self) -> Optional[int]:
        return self._proc.pid()

    def wait(self) -> Optional[int]:
        return self._proc.wait()

    def kill(self) -> None:
        # If we've already killed everything, don't do it again.
        if self._killed:
            return

        # If we weren't able to run perf record, then we don't have anything to
        # do here.
        if self._perf_record is None:
            self._proc.kill()
            self._killed = True
            return

        self._perf_record.kill()
        self._bench.popen(
            host=self._host,
            label=f'{self._label}_create_java_perf_map',
            cmd=['create-java-perf-map.sh',
                 str(self._proc.pid())]).wait()
        self._bench.popen(
            host=self._host,
            label=f'{self._label}_create_java_perf_map_mv',
            cmd=[
                'sudo', 'mv', f'/tmp/perf-{self._proc.pid()}.map',
                self._bench.abspath(f'perf-{self._proc.pid()}.map')
            ]).wait()
        self._proc.kill()
        self._killed = True
