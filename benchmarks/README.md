# Benchmarks

This directory contains FrankenPaxos benchmarks. Benchmarks use mininet, which
requires sudo, so you'll have to run the benchmarks with sudo. However, you
have to be a bit careful running the python program as sudo. As sudo, you may
have a different python version, a different python path, a different path, and
so on. Thus, we recommend you run benchmarks like this:

```bash
sudo PATH="$PATH" PYTHONPATH="$PYTHONPATH" "$(which python)" \
    -m benchmarks.multipaxos \
    --jar jvm/target/scala-2.12/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar \
    -s /tmp
```

`PATH="$PATH" PYTHONPATH="$PYTHONPATH"` ensures that you keep the same PATH and
PYTHONPATH as sudo. `$(which python)` ensures that you use the same python
version. `-m benchmarks.multipaxos` specifies which benchmark to run. The rest
is flags to the benchmark.

Alternatively, you can use the `sudopython` script which does this for you:

```bash
sudopython -m benchmarks.multipaxos \
    --jar jvm/target/scala-2.12/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar \
    -s /tmp
```
