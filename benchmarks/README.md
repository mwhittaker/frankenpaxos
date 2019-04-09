# Benchmarks
This directory contains FrankenPaxos benchmarks written in Python 3.

## Getting Started
We highly recommend that you run these benchmarks from within a fresh conda
environment or virtualenv or something similar. For example:

```bash
conda create --name frankenpaxos python=3.6
source activate frankenpaxos
pip install --upgrade pip
pip install -r benchmarks/requirements.txt
```

The benchmarks use mininet for network emulation, and mininet requires sudo, so
you'll have to run the benchmarks with sudo. However, you have to be a bit
careful running the python program as sudo. As sudo, you may have a different
python version, a different python path, a different path, and so on. Thus, we
recommend you run benchmarks using the `sudopython` script. `sudopython` takes
care of all of this for you.

```bash
./scripts/sudopython -m benchmarks.echo.echo
./scripts/sudopython -m benchmarks.fastmultipaxos.fastmultipaxos
```
