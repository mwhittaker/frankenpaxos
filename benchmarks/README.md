# Benchmarks
This directory contains benchmarks written in Python 3. We now describe how the
benchmarks work.

## Overview
Consider the unreplicated state machine protocol in
`shared/src/main/scala/frankenpaxos/unreplicated`. We have a single
unreplicated server that manages a state machine, and we have any number of
clients. A client can send a state machine command to the server, and when the
server receives the command, the state machine executes the command, and the
server returns the result of executing the command back to the client.

A javascript visualization of this protocol can be found
[here](https://mwhittaker.github.io/frankenpaxos/js/src/main/js/unreplicated/unreplicated.html).
In this example, the state machine is an array of strings. Clients send strings
to the server, and the server appends the strings.

Imagine we want to deploy this protocol on EC2 with the server on one machine
and every client on its own machine. We could deploy the protocol by hand,
ssh'ing into each machine and manually running all the executables making sure
to pass in all the right command line flags and whatnot. This is super tedious
and super error-prone. Instead, we have a python script do all the heavy
lifting for us. We pass the python script a list of the IP addresses of our
EC2 machines, the JAR file we want to execute, and a couple other flags, and
the script takes care of ssh'ing in to every machine and running the right
commands. That looks something like this:

```
 1.1.1.1                      1.1.1.2
+--------+       SSH         +--------+
| python | ----------------> | server |
| script |                   |        |
+--------+                   +--------+
     \
      \
       \________________SSH______________
           \   1.1.1.3    \   1.1.1.4    \   1.1.1.5
            \ +--------+   \ +--------+   \ +--------+
             \| client |    \| client |    \| client |
              |        |     |        |     |        |
              +--------+     +--------+     +--------+
```

Here, we have five machines with the fictional IP addresses 1.1.1.1, 1.1.1.2,
..., 1.1.1.5. We run the Python script on 1.1.1.1, and the script ssh's into
the other machines to launch the server and three clients. The protocol then
runs for some time, with clients and servers communicating with one another
over TCP or UDP or whatever it is they use to communicate with one another.
That looks something like this:

```
 1.1.1.1                      1.1.1.2
+--------+                   +--------+
| python |                   | server |
| script |                   |        |
+--------+                   +--------+
                                /|\
                    ___________/ | \___________
                   /             |             \
               1.1.1.3        1.1.1.4        1.1.1.5
              +--------+     +--------+     +--------+
              | client |     | client |     | client |
              |        |     |        |     |        |
              +--------+     +--------+     +--------+
```

When we run benchmarks, we assume that every node (e.g., the server and every
client in this example) has access to a shared file system. If we run a
benchmark locally on our laptop (i.e. we run every node on our laptop using
localhost), then this shared file system is just our laptop's file system. When
we run a benchmark across multiple machines on EC2, we use EFS as the shared
file system. That is, every machine mounts a shared EFS file system.

As the clients and server run, they record information about the execution of
the benchmark in a directory in the shared file system (more on this later).
Later, we can read the data, analyze it, plot it, etc.

## A Minimal Working Example
We can run this exact scenario for real on our local machine. Make sure you've
run `frankenpaxosJVM/assembly` in sbt and then run the following from the
frankenpaxos directory where `~/.ssh/id_rsa` is the private SSH key you can use
to run `ssh localhost`:

```bash
python -m benchmarks.unreplicated.smoke \
    -s /tmp \
    -i ~/.ssh/id_rsa \
    --cluster benchmarks/unreplicated/local_cluster.json
```

If everything runs correctly, you should see some output that looks something
like this:

```
Running suite in /tmp/2021-04-07_18:41:18.893020_HAOSRTGXOI_unreplicated_smoke
[001/003; 33.33%] 0:00:08 / 0:00:08 + 0:00:16? {'num_client_procs': 1, 'num_clients_per_proc': 1, 'latency.median_ms': '0.103715', 'start_throughput_1s.p90': '16212.0'}
[002/003; 66.67%] 0:00:08 / 0:00:16 + 0:00:08? {'num_client_procs': 2, 'num_clients_per_proc': 1, 'latency.median_ms': '0.143476', 'start_throughput_1s.p90': '11784.0'}
[003/003; 100.0%] 0:00:10 / 0:00:26 + 0:00:00? {'num_client_procs': 3, 'num_clients_per_proc': 1, 'latency.median_ms': '0.52744', 'start_throughput_1s.p90': '3127.6'}
```

Let's dissect what's happening. `benchmarks/unreplicated/unreplicated.py`
contains code that specifies how to ssh into a cluster of machines to run the
unreplicated server and clients. There's nothing magic about the script; it
literally just has the command you would have to run in a shell to run the
server and the clients, and has some code to run these commands over ssh. In
general, every protocol has a file like this (e.g.,
`benchmarks/multipaxos/multipaxos.py`, `benchmarks/epaxos/epaxos.py`, etc.)

Note though that `unreplicated.py` is not actually executable. It contains all
the code needed to run an experiment, but it doesn't actually run any specific
experiment. Instead, we create separate scripts, one per experiment, that use
the `unreplicated.py` code to run a specific experiment.
`benchmarks/unreplicated/smoke.py`, for example, is a [smoke
test](https://en.wikipedia.org/wiki/Smoke_testing_(software)). It's a dirt
simple script that just makes sure our code runs at all.

If you peek inside the script, you can see the specific parameters of the
experiment:

```python
return [
    Input(
        num_client_procs=num_client_procs,
        num_warmup_clients_per_proc=1,
        num_clients_per_proc=1,
        jvm_heap_size='100m',
        measurement_group_size=1,
        warmup_duration=datetime.timedelta(seconds=2),
        warmup_timeout=datetime.timedelta(seconds=3),
        warmup_sleep=datetime.timedelta(seconds=0),
        duration=datetime.timedelta(seconds=2),
        timeout=datetime.timedelta(seconds=3),
        client_lag=datetime.timedelta(seconds=0),
        state_machine='Noop',
        workload=workload.StringWorkload(size_mean=1, size_std=0),
        profiled=args.profile,
        monitored=args.monitor,
        prometheus_scrape_interval=datetime.timedelta(
            milliseconds=200),
        client_options=ClientOptions(),
        client_log_level=args.log_level,
        server_options=ServerOptions(),
        server_log_level=args.log_level,
    )
    for num_client_procs in [1, 2, 3]
]
```

Here, our experiment (also called a _benchmark suite_) has three benchmarks.
The first benchmark runs 1 client, the second runs 2 clients, and the third
runs 3 clients. All three benchmarks use the 'Noop' state machine, they run for
2 seconds with a 2 second warmup, they use a JVM heap size of 100 MB, etc.

Now, let's look at the arguments we pass to the script.

- `-s tmp` specifies that every benchmark in the suite should write to a
  directory in `/tmp`. If we were running on EC2, we wouldn't use `/tmp`.
  Instead, we'd pass in a mounted EFS file system. We'll discuss exactly what
  is written into this directory in a moment.
- `-i ~/.ssh/id_rsa` is the key we use to SSH. You should be able to run `ssh
  -i ~/.ssh/id_rsa localhost` (or `ssh -i ~/.ssh/id_rsa
  $SOME_EC2_PRIVATE_IP_ADDRESS`) without getting prompted for a password. If
  that command doesn't run successfully, or it prompts you for a password, the
  script probably won't run correctly. Remember that the scripts are just
  running ssh under the hood.
- `--cluster benchmarks/unreplicated/local_cluster.json` is a JSON file
  specifying the IP addresses of the machines on which we run the benchmark
  suite. It looks like this.

        {
          "1": {
            "clients": ["localhost"],
            "server": ["localhost"]
          }
        }

  Here, the "1" maps to the IP addresses that we use when `f = 1` (recall that
  we tolerate at most `f` faults). We specify that the clients and server both
  use localhost. Note that we can run more than one client, but we only need to
  specify one IP address for the clients. The scripts are smart enough to
  handle that (see the scripts for details). If we were running on EC2, we
  wouldn't use localhost, and we would list multiple IP addresses for the
  clients.

You can pass the `--help` flag to the script to see the other flags.
Notice, for example, that the script used the default location of the JAR file
(i.e.
`frankenpaxos/jvm/target/scala-2.12/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar`).

## What Data Gets Written (a.k.a. How Do I Debug This Mess?)
Benchmarks write data, logs, and debugging information to the shared file
system. We now explore exactly what is written and how to read through things
to debug stuff. When we ran the unreplicated smoke test above, the script
printed out that it was writing stuff to
`/tmp/2021-04-07_18:41:18.893020_HAOSRTGXOI_unreplicated_smoke`. Every
benchmark suite writes data to its own directory. The directory names start
with the time at which the benchmark started, and they all include a unique
string that you can use to `cd` a little more easily:

```
$ cd /tmp/*HAOS*
$ ls
001
002
003
args.json
inputs.txt
results.csv
start_time.txt
stop_time.txt
```

Inside the suite directory, there are a number of files that contain
information about the suite. `args.json` contains the flags we passed to the script:

```
$ cat args.json
{
    "suite_directory": "/tmp",
    "jar": "/home/vagrant/frankenpaxos/benchmarks/../jvm/target/scala-2.12/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar",
    "log_level": "debug",
    "profile": false,
    "monitor": false,
    "address": null,
    "cluster": "benchmarks/unreplicated/local_cluster.json",
    "identity_file": "/home/vagrant/.ssh/id_rsa"
}
```

`inputs.txt` contains the input parameters for every benchmark (one line per
benchmark).

```
$ cat inputs.txt
Input(num_client_procs=1, num_warmup_clients_per_proc=1, num_clients_per_proc=1, jvm_heap_size='100m', measurement_group_size=1, warmup_duration=datetime.timedelta(0, 2), warmup_timeout=datetime.timedelta(0, 3), warmup_sleep=datetime.timedelta(0), duration=datetime.timedelta(0, 2), timeout=datetime.timedelta(0, 3), client_lag=datetime.timedelta(0), state_machine='Noop', workload=StringWorkload(size_mean=1, size_std=0, name='StringWorkload'), profiled=False, monitored=False, prometheus_scrape_interval=datetime.timedelta(0, 0, 200000), client_options=ClientOptions(), client_log_level='debug', server_options=ServerOptions(flush_every_n=1), server_log_level='debug')
Input(num_client_procs=2, num_warmup_clients_per_proc=1, num_clients_per_proc=1, jvm_heap_size='100m', measurement_group_size=1, warmup_duration=datetime.timedelta(0, 2), warmup_timeout=datetime.timedelta(0, 3), warmup_sleep=datetime.timedelta(0), duration=datetime.timedelta(0, 2), timeout=datetime.timedelta(0, 3), client_lag=datetime.timedelta(0), state_machine='Noop', workload=StringWorkload(size_mean=1, size_std=0, name='StringWorkload'), profiled=False, monitored=False, prometheus_scrape_interval=datetime.timedelta(0, 0, 200000), client_options=ClientOptions(), client_log_level='debug', server_options=ServerOptions(flush_every_n=1), server_log_level='debug')
Input(num_client_procs=3, num_warmup_clients_per_proc=1, num_clients_per_proc=1, jvm_heap_size='100m', measurement_group_size=1, warmup_duration=datetime.timedelta(0, 2), warmup_timeout=datetime.timedelta(0, 3), warmup_sleep=datetime.timedelta(0), duration=datetime.timedelta(0, 2), timeout=datetime.timedelta(0, 3), client_lag=datetime.timedelta(0), state_machine='Noop', workload=StringWorkload(size_mean=1, size_std=0, name='StringWorkload'), profiled=False, monitored=False, prometheus_scrape_interval=datetime.timedelta(0, 0, 200000), client_options=ClientOptions(), client_log_level='debug', server_options=ServerOptions(flush_every_n=1), server_log_level='debug')
```

`results.csv` contains the results of every benchmark (one line per benchmark).

```
$ cat results.txt
Input(num_client_procs=1, num_warmup_clients_per_proc=1, num_clients_per_proc=1, jvm_heap_size='100m', measurement_group_size=1, warmup_duration=datetime.timedelta(0, 2), warmup_timeout=datetime.timedelta(0, 3), warmup_sleep=datetime.timedelta(0), duration=datetime.timedelta(0, 2), timeout=datetime.timedelta(0, 3), client_lag=datetime.timedelta(0), state_machine='Noop', workload=StringWorkload(size_mean=1, size_std=0, name='StringWorkload'), profiled=False, monitored=False, prometheus_scrape_interval=datetime.timedelta(0, 0, 200000), client_options=ClientOptions(), client_log_level='debug', server_options=ServerOptions(flush_every_n=1), server_log_level='debug')
Input(num_client_procs=2, num_warmup_clients_per_proc=1, num_clients_per_proc=1, jvm_heap_size='100m', measurement_group_size=1, warmup_duration=datetime.timedelta(0, 2), warmup_timeout=datetime.timedelta(0, 3), warmup_sleep=datetime.timedelta(0), duration=datetime.timedelta(0, 2), timeout=datetime.timedelta(0, 3), client_lag=datetime.timedelta(0), state_machine='Noop', workload=StringWorkload(size_mean=1, size_std=0, name='StringWorkload'), profiled=False, monitored=False, prometheus_scrape_interval=datetime.timedelta(0, 0, 200000), client_options=ClientOptions(), client_log_level='debug', server_options=ServerOptions(flush_every_n=1), server_log_level='debug')
Input(num_client_procs=3, num_warmup_clients_per_proc=1, num_clients_per_proc=1, jvm_heap_size='100m', measurement_group_size=1, warmup_duration=datetime.timedelta(0, 2), warmup_timeout=datetime.timedelta(0, 3), warmup_sleep=datetime.timedelta(0), duration=datetime.timedelta(0, 2), timeout=datetime.timedelta(0, 3), client_lag=datetime.timedelta(0), state_machine='Noop', workload=StringWorkload(size_mean=1, size_std=0, name='StringWorkload'), profiled=False, monitored=False, prometheus_scrape_interval=datetime.timedelta(0, 0, 200000), client_options=ClientOptions(), client_log_level='debug', server_options=ServerOptions(flush_every_n=1), server_log_level='debug')
```

`start_time.txt` and `stop_time.txt` contain the start and stop time of the
suite.

```
$ cat start_time.txt
2021-04-07 18:41:18.893341
$ cat stop_time.txt
2021-04-07 18:41:45.525412
```

Finally, we have one directory for every benchmark. We ran three benchmarks
(with 1, 2, and 3 clients), so we have three directories: `001/`, `002/`, and
`003/`. Let's look inside `001/`.

```
$ cd 001/
$ ls
client_0_cmd.txt
client_0_err.txt
client_0_out.txt
client_0_returncode.txt
input.json
input.txt
log.txt
pids.json
server_cmd.txt
server_err.txt
server_out.txt
server_returncode.txt
start_time.txt
stop_time.txt
workload.pbtxt
```

`input.json` and `input.txt` include the parameters of the benchmark. Recall
that these were specified in `smoke.py`.

```
$ cat input.json
{
    "num_client_procs": 1,
    "num_warmup_clients_per_proc": 1,
    "num_clients_per_proc": 1,
    "jvm_heap_size": "100m",
    "measurement_group_size": 1,
    "warmup_duration": "0:00:02",
    "warmup_timeout": "0:00:03",
    "warmup_sleep": "0:00:00",
    "duration": "0:00:02",
    "timeout": "0:00:03",
    "client_lag": "0:00:00",
    "state_machine": "Noop",
    "workload": {
        "size_mean": 1,
        "size_std": 0,
        "name": "StringWorkload"
    },
    "profiled": false,
    "monitored": false,
    "prometheus_scrape_interval": "0:00:00.200000",
    "client_options": {},
    "client_log_level": "debug",
    "server_options": {
        "flush_every_n": 1
    },
    "server_log_level": "debug"
}
$ cat input.txt
Input(num_client_procs=1, num_warmup_clients_per_proc=1, num_clients_per_proc=1, jvm_heap_size='100m', measurement_group_size=1, warmup_duration=datetime.timedelta(0, 2), warmup_timeout=datetime.timedelta(0, 3), warmup_sleep=datetime.timedelta(0), duration=datetime.timedelta(0, 2), timeout=datetime.timedelta(0, 3), client_lag=datetime.timedelta(0), state_machine='Noop', workload=StringWorkload(size_mean=1, size_std=0, name='StringWorkload'), profiled=False, monitored=False, prometheus_scrape_interval=datetime.timedelta(0, 0, 200000), client_options=ClientOptions(), client_log_level='debug', server_options=ServerOptions(flush_every_n=1), server_log_level='debug')
```

`log.txt` contains a log of the benchmark execution.

```
$ cat log.txt
[Wednesday April 07, 18:41:19.472150] Servers started.
[Wednesday April 07, 18:41:19.472220] Client lag ended.
[Wednesday April 07, 18:41:19.577963] Clients started and running for 0:00:02.
[Wednesday April 07, 18:41:26.793130] Clients finished and processes terminated.
[Wednesday April 07, 18:41:26.793205] Reading recorder data from the following CSVs:
[Wednesday April 07, 18:41:26.793223] - /tmp/2021-04-07_18:41:18.893020_HAOSRTGXOI_unreplicated_smoke/001/client_0_data.csv
[Wednesday April 07, 18:41:27.025315] Recorder data read.
[Wednesday April 07, 18:41:27.025360] Setting aggregate recorder data index.
[Wednesday April 07, 18:41:27.026534] Aggregate recorder data index set.
[Wednesday April 07, 18:41:27.026560] Sorting aggregate recorder data on index.
[Wednesday April 07, 18:41:27.026977] Aggregate recorder data sorted on index.
[Wednesday April 07, 18:41:27.027000] Removing /tmp/2021-04-07_18:41:18.893020_HAOSRTGXOI_unreplicated_smoke/001/client_0_data.csv.
[Wednesday April 07, 18:41:27.027241] Individual recorder data removed.
[Wednesday April 07, 18:41:27.027549] Dropping prefix of aggregate recorder data.
[Wednesday April 07, 18:41:27.030628] Prefix of aggregate recorder data dropped.
[Wednesday April 07, 18:41:27.031576] Computing on aggregate recorder data for write.
[Wednesday April 07, 18:41:27.033836] - Computing latency.
[Wednesday April 07, 18:41:27.040284] - Latency computed.
[Wednesday April 07, 18:41:27.040326] - Computing 1 second start throughput.
[Wednesday April 07, 18:41:27.045989] - 1 second start throughput computed.
[Wednesday April 07, 18:41:27.046033] Aggregate recorder data for write computed.
```

For every process, we record the command used to launch the process as well as
the standard out, standard error, and return code of the process. In the first
benchmark, we run 1 client process and 1 server process. We can inspect the
client like this:

```
$ cat client_0_cmd.txt
echo NCLEJFKCRHRCEOEWTSOSVXDBWAUHHOQYOESIAYAUZFGQIKJEZVXVAZODNESTYQJHVUQDCHZNAAXECPHF; (java -cp /home/vagrant/frankenpaxos/jvm/target/scala-2.12/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar frankenpaxos.unreplicated.ClientMain --host 127.0.0.1 --port 10000 --server_host 127.0.0.1 --server_port 10100 --log_level debug --prometheus_host 127.0.0.1 --prometheus_port -1 --measurement_group_size 1 --warmup_duration 2.0s --warmup_timeout 3.0s --warmup_sleep 0.0s --num_warmup_clients 1 --duration 2.0s --timeout 3.0s --num_clients 1 --workload /tmp/2021-04-07_18:41:18.893020_HAOSRTGXOI_unreplicated_smoke/001/workload.pbtxt --output_file_prefix /tmp/2021-04-07_18:41:18.893020_HAOSRTGXOI_unreplicated_smoke/001/client_0) 2> "/tmp/2021-04-07_18:41:18.893020_HAOSRTGXOI_unreplicated_smoke/001/client_0_err.txt" > "/tmp/2021-04-07_18:41:18.893020_HAOSRTGXOI_unreplicated_smoke/001/client_0_out.txt"
$
$ cat client_0_out.txt
[Apr 07 18:41:20.570000000] [INFO] [Thread 1] Actor frankenpaxos.unreplicated.Client@77f1baf5 registering on address NettyTcpAddress(/127.0.0.1:10000).
[Apr 07 18:41:20.647000000] [INFO] [Thread 1] Client warmup started.
[Apr 07 18:41:20.654000000] [DEBUG] [Thread 10] No channel was found between NettyTcpAddress(/127.0.0.1:10000) and NettyTcpAddress(/127.0.0.1:10100), so we are creating one.
[Apr 07 18:41:20.696000000] [DEBUG] [Thread 10] Attempted to send a message from NettyTcpAddress(/127.0.0.1:10000) to NettyTcpAddress(/127.0.0.1:10100), but a channel is currently pending. The message is being buffered for later.
[Apr 07 18:41:20.698000000] [INFO] [Thread 10] Client socket on address /127.0.0.1:33690 (a.k.a. NettyTcpAddress(/127.0.0.1:10000)) established connection with /127.0.0.1:10100.
[Apr 07 18:41:20.698000000] [DEBUG] [Thread 10] A channel between local address NettyTcpAddress(/127.0.0.1:10000) and remote address NettyTcpAddress(/127.0.0.1:10100) is being registered, and a set of 2 pending messages was found. We are sending the pending messages along the channel.
[Apr 07 18:41:22.646000000] [INFO] [Thread 1] Client warmup finished successfully.
[Apr 07 18:41:22.655000000] [INFO] [Thread 1] Clients started.
[Apr 07 18:41:24.655000000] [INFO] [Thread 1] Clients finished successfully.
[Apr 07 18:41:24.656000000] [INFO] [Thread 1] Shutting down transport.
[Apr 07 18:41:24.656000000] [INFO] [Thread 1] Transport shut down.
[Apr 07 18:41:24.659000000] [DEBUG] [Thread 10] Successfully unregistered channel between local address NettyTcpAddress(/127.0.0.1:10000) and remote address NettyTcpAddress(/127.0.0.1:10100).
$
$ cat client_0_err.txt
$
$ cat client_0_returncode.txt
0
```

If you run a script, and it crashes, this is the best place to look to figure
out what went wrong. Did the process launch at all? If not, can you copy and
paste the command into a terminal and run it successfully? If it did launch
successfully, what errors did it print? What was its return code? How far did
it get in its execution?

Let's walk through an example. Let's say we modify `smoke.py` to include the
following:

```python
def inputs(self) -> Collection[Input]:
    return [
        Input(
            num_client_procs=num_client_procs,
            num_warmup_clients_per_proc=1,
            num_clients_per_proc=1,
            jvm_heap_size='100f',
            measurement_group_size=1,
            warmup_duration=datetime.timedelta(seconds=2),
            warmup_timeout=datetime.timedelta(seconds=3),
            warmup_sleep=datetime.timedelta(seconds=0),
            duration=datetime.timedelta(seconds=2),
            timeout=datetime.timedelta(seconds=3),
            client_lag=datetime.timedelta(seconds=0),
            state_machine='Noop',
            workload=workload.StringWorkload(size_mean=1, size_std=0),
            profiled=args.profile,
            monitored=args.monitor,
            prometheus_scrape_interval=datetime.timedelta(
                milliseconds=200),
            client_options=ClientOptions(),
            client_log_level=args.log_level,
            server_options=ServerOptions(),
            server_log_level=args.log_level,
        )

        for num_client_procs in [1, 2, 3]
    ]
```

When we run the script, it crashes with the following error:

```
/home/vagrant/install/anaconda3/envs/frankenpaxos/lib/python3.6/site-packages/requests/__init__.py:91: RequestsDependencyWarning: urllib3 (1.25.1) or chardet (3.0.4) doesn't match a
supported version!
  RequestsDependencyWarning)
Running suite in /tmp/2021-04-07_19:22:38.030051_SCPYVKZCRO_unreplicated_smoke.
Traceback (most recent call last):
  File "/home/vagrant/install/anaconda3/envs/frankenpaxos/lib/python3.6/runpy.py", line 193, in _run_module_as_main
    "__main__", mod_spec)
  File "/home/vagrant/install/anaconda3/envs/frankenpaxos/lib/python3.6/runpy.py", line 85, in _run_code
    exec(code, run_globals)
  File "/home/vagrant/frankenpaxos/benchmarks/unreplicated/smoke.py", line 53, in <module>
    main(get_parser().parse_args())
  File "/home/vagrant/frankenpaxos/benchmarks/unreplicated/smoke.py", line 49, in main
    suite.run_suite(dir)
  File "/home/vagrant/frankenpaxos/benchmarks/benchmark.py", line 268, in run_suite
    output = self.run_benchmark(bench, args, input)
  File "/home/vagrant/frankenpaxos/benchmarks/unreplicated/unreplicated.py", line 273, in run_benchmark
    save_data=False)['write']
  File "/home/vagrant/frankenpaxos/benchmarks/benchmark.py", line 431, in parse_labeled_recorder_data
    df = _wrangle_recorder_data(bench, filenames, drop_prefix, save_data)
  File "/home/vagrant/frankenpaxos/benchmarks/benchmark.py", line 397, in _wrangle_recorder_data
    start_time = df.index[0]
  File "/home/vagrant/install/anaconda3/envs/frankenpaxos/lib/python3.6/site-packages/pandas/core/indexes/base.py", line 3958, in __getitem__
    return getitem(key)
IndexError: index 0 is out of bounds for axis 0 with size 0
```

What in the world does this mean? Let's take a look at the suite directory.

```
$ cd /tmp/*SCPYV*
$ cd 001
$ cat server_err.txt
Invalid initial heap size: -Xms100f
Error: Could not create the Java Virtual Machine.
Error: A fatal exception has occurred. Program will exit.
```

The server's standard error reports that we passed in an invalid heap size. We
can check the command that got run to confirm.

```
$ cat server_cmd.txt
echo JKLRZCAJCZOQGMWGLXJUDDEHBYXUGTKNZLOXEDOLORRAKMWEFLXLSPCDDNLGFIBIYXKIVPYNGRMVYOZL; (java -Xms100f -Xmx100f -cp /home/vagrant/frankenpaxos/jvm/target/scala-2.12/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar frankenpaxos.unreplicated.ServerMain --host 127.0.0.1 --port 10100 --log_level debug --state_machine Noop --prometheus_host 127.0.0.1 --prometheus_port -1 --options.flushEveryN 1) 2> "/tmp/2021-04-07_19:22:38.030051_SCPYVKZCRO_unreplicated_smoke/001/server_err.txt" > "/tmp/2021-04-07_19:22:38.030051_SCPYVKZCRO_unreplicated_smoke/001/server_out.txt"
```

Yup, we passed in `-Xmx100f` which is not a valid heap size. It should have
been `100m`. If we fix this in `smoke.py` and run it again, things work fine.

## Getting Started
The benchmarks use [Prometheus](https://prometheus.io/) to monitor code and
[Grafana](https://grafana.com/) to analyze monitoring information. You might
want to install these, but if you don't want to perform any monitoring, then
you don't need to.

We highly recommend that you run the benchmarks from within a fresh conda
environment or virtualenv or something similar. For example:

```bash
conda create --name frankenpaxos python=3.6
source activate frankenpaxos
pip install --upgrade pip
pip install -r benchmarks/requirements.txt
```

## Running on EC2
To run on EC2, create an EFS file system, launch a bunch of machines, and then
run the following on each of the machines. You'll have to install Java and
Scala however you like.

```bash
sudo apt-get update && sudo apt-get upgrade
sudo apt-get install git tmux vim
# Install dotfiles if wanted.
# Install Java (https://github.com/mwhittaker/vms/blob/master/install_java8.sh)
# Install Scala (https://github.com/mwhittaker/vms/blob/master/install_scala.sh)
# Install efs-utils (https://docs.aws.amazon.com/efs/latest/ug/gs-step-three-connect-to-ec2-instance.html)
git clone git@github.com:aws/efs-utils.git
cd efs-utils
sudo apt-get install binutils nfs-common stunnel4
./build-deb.sh
sudo dpkg -i ./build/amazon-efs-utils*.deb
sudo mkdir /mnt/efs
# Use your EFS file system id here.
sudo mount -t efs <fs-id> /mnt/efs
```

Then, on one of the machines, run something like looks like this:

```
cp jvm/target/scala-2.12/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar /mnt/efs/tmp/ && python -m benchmarks.<some_benchmark> -j /mnt/efs/tmp/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar -s /mnt/efs/tmp/ -m -l info -i <path_to_ssh_key> --cluster <path_to_cluster_file>
```

Assuming you've mounted the EFS file system on `/mnt/efs` and made a
`/mnt/efs/tmp` directory, this will run `<some_benchmark>` (e.g.,
`unreplicated.smoke`) and write the results to `/mnt/efs/tmp`. You'll have to
populate `<path_to_cluster_file>` with the private IP addresses of the EC2
machines. Every benchmark directory has an example cluster file for reference.

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
