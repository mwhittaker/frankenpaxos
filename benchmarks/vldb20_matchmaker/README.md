# Matchmaker Paxos

This directory contains the benchmark plotting code for Matchmaker Paxos. The
benchmarks themselves are in the `matchmakermultipaxos` directory and can be
run from the root `frankenpaxos/` directory like this.

```bash
python -m benchmarks.matchmakermultipaxos.vldb_ablation --help
python -m benchmarks.matchmakermultipaxos.vldb_chaos --help
python -m benchmarks.matchmakermultipaxos.vldb_leader_failure --help
python -m benchmarks.matchmakermultipaxos.vldb_leader_reconfiguration --help
python -m benchmarks.matchmakermultipaxos.vldb_lt --help
python -m benchmarks.matchmakermultipaxos.vldb_matchmaker_reconfiguration --help
```

See the `README.md` in the `frankenpaxos/benchmarks/` directory for more
information on running benchmarks.

An example cluster file for Matchmaker Paxos is given in
`matchmakermultipaxos/local_cluster.json`. That file looks like this:

```
{
  "1": {
    "clients": ["localhost"],
    "leaders": ["localhost"],
    "matchmakers": ["localhost"],
    "reconfigurers": ["localhost"],
    "acceptors": ["localhost"],
    "replicas": ["localhost"],
    "driver": ["localhost"]
  },
  "2": {
    "clients": ["localhost"],
    "leaders": ["localhost"],
    "matchmakers": ["localhost"],
    "reconfigurers": ["localhost"],
    "acceptors": ["localhost"],
    "replicas": ["localhost"],
    "driver": ["localhost"]
  }
}
```

The `"1"` and `"2"` here correspond to the value of `f`--the maximum number of
allowable failures. A more realistic cluster file (for `f=1` might look like
this (the IP addresses here are made up):

```
{
  "1": {
    "clients": [
        "1.1.1.1",
        "1.1.1.2",
        "1.1.1.3",
        "1.1.1.4",
        "1.1.1.5",
        "1.1.1.6"
    ],
    "leaders": [
        "1.1.1.7",
        "1.1.1.8",
        "1.1.1.9"
    ],
    "matchmakers": [
        "1.1.1.10",
        "1.1.1.11",
        "1.1.1.12"
    ],
    "reconfigurers": [
        "1.1.1.13",
        "1.1.1.14",
        "1.1.1.15",
        "1.1.1.16"
    ],
    "acceptors": [
        "1.1.1.17",
        "1.1.1.18",
        "1.1.1.19"
    ],
    "replicas": [
        "1.1.1.20",
        "1.1.1.21",
        "1.1.1.22",
        "1.1.1.23"
    ],
    "driver": [
        "1.1.1.24"
    ]
  }
}
```

The exact number of each node that you'll need depends on the benchmark. For
example, in `matchmakermultipaxos/vldb_lt.py`, we see the following:

```
f = 1,
num_client_procs = num_client_procs,
num_warmup_clients_per_proc = num_clients_per_proc,
num_clients_per_proc = num_clients_per_proc,
num_leaders = 2,
num_matchmakers = 3,
num_reconfigurers = 2,
num_acceptors = 3,
num_replicas = 3,
```

This tells us that `f` is 1 and that we'll need 2 leaders, 3 matchmakers, 2
reconfigurers, 3 acceptors, 3 replicas, and 1 driver (there's only ever 1
driver). The number of clients is varied throughout the benchmark. In the same
file, we see the following:

```python
for (num_client_procs, num_clients_per_proc) in [
    (1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (1, 10),
    (1, 25), (1, 50), (1, 75), (1, 100), (2, 100), (3, 100),
    (4, 100), (5, 100),
]
```

The number of client machines varies from 1 to 5, so we'll need 5 client
machines.

If the experiment needs 4 clients, for example, and you only list 2 in the
cluster file, that's ok. The benchmark will run 2 clients on each machine.
Generally, if you have fewer machines than are required, things will still
work, though the throughput might be impacted by co-locating nodes together.
