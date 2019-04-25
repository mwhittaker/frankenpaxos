# Benchmarks
This directory contains FrankenPaxos benchmarks written in Python 3.

## Dependencies
The FrankenPaxos benchmarks use [mininet](http://mininet.org/download/) for
network emulation, so you'll have to install that. The benchmarks also depend
on a couple of executables for profiling and monitoring, which you'll have to
install. For profiling:

- [perf](https://perf.wiki.kernel.org/index.php/Tutorial) is used to profile
  code.
- [FlameGraph](https://github.com/brendangregg/FlameGraph) is used to visualize
  the profiling information.
- [perf-map-agent](https://github.com/jvm-profiling-tools/perf-map-agent) is
  needed to accurately profile JVM code.
- [FlameScope](https://github.com/Netflix/flamescope) is also used to visualize
  profiling information.

For monitoring:

- [Prometheus](https://prometheus.io/) is used to monitor code.
- [Grafana](https://grafana.com/) is used to analyze monitoring information.

Note that if you don't want to perform any profiling or monitoring, then you
don't need to install these dependencies.

## Getting Started
We highly recommend that you run the benchmarks from within a fresh conda
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

Thus, you can a benchmark from the `frankenpaxos` directory like this:

```bash
./scripts/sudopython -m benchmarks.echo.echo
```

If you want to run a benchmark script that doesn't require sudo (e.g., script
to plot data), then you don't need to use `sudopython`:

```bash
python -m benchmarks.plot_latency_and_throughput --help
```

If you're not in the `frankenpaxos` directory, you need to ensure that the
`frankenpaxos` directory is in your `PYTHONPATH`. Otherwise, python won't know
where to find the benchmarks. A convenient way to do that is like this:

```bash
PYTHONPATH="path/to/frankenpaxos:$PYTHONPATH" \
    python -m benchmarks.plot_latency_and_throughput --help
```

## Analyzing Benchmarks
To dissect the performance of a particular benchmark, we can use Prometheus and
Grafana. When you run a benchmark with monitoring enabled (typically by passing
the `-m` flag), Prometheus metrics are recorded into the `prometheus_data`
directory of the benchmark's directory. We run a Prometheus server to serve
these metrics and a Grafana server (that reads from the Prometheus server) to
graph the metrics.

First, open `grafana/dashboards/dashboards.yml`, and update the `path`
setting at the bottom of the file. `path` should be the absolute path of the
`grafana/dashboards/` directory. Next, head to the directory in which you
installed Grafana and start a Grafana server like this:

```bash
GF_SERVER_HTTP_PORT=8004 \
GF_PATHS_PROVISIONING=path/to/frankenpaxos/grafana \
./bin/grafana-server web
```

where `path/to/frankenpaxos/grafana` is the absolute path to the
`frankenpaxos/grafana` directory. The `frankenpaxos/grafana` directory contains
all the configuration files that Grafana needs.

Finally, head to a benchmark directory and start a Prometheus server like this:

```bash
prometheus \
    --config.file=<(echo "") \
    --storage.tsdb.path=prometheus_data \
    --web.listen-address=0.0.0.0:8003
```
