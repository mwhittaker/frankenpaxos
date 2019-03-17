# Benchmarks

This directory contains FrankenPaxos benchmarks. Benchmarks use mininet, which
requires sudo, so you'll have to run the benchmarks with sudo. However, you
have to be a bit careful running the python program as sudo. As sudo, you may
have a different python version, a different python path, a different path, and
so on. Thus, we recommend you run benchmarks like this:

```bash
sudo PATH="$PATH" PYTHONPATH="$PYTHONPATH" "$(which python)" \
    -m benchmarks.fastmultipaxos
```

`PATH="$PATH" PYTHONPATH="$PYTHONPATH"` ensures that you keep the same PATH and
PYTHONPATH as sudo. `$(which python)` ensures that you use the same python
version. `-m benchmarks.fastmultipaxos` specifies which benchmark to run. The
rest is flags to the benchmark.

Alternatively, you can use the `sudopython` script which does this for you:

```bash
sudopython -m benchmarks.fastmultipaxos
```
