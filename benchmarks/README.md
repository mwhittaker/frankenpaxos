# Benchmarks
This directory contains benchmarks written in Python 3.

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
lifting for us.  We pass the python script a list of the IP addresses of our
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

The protocol then runs for some time, with clients and servers communicating
with one another over TCP or UDP or whatever it is they use to communicate with
one another.

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

When we run benchmarks, we assume that every machine mounts a shared EFS file
system. As the clients and server run, they record information about the
execution of the benchmark in a directory in the EFS mount. Later, we can read
the data from the EFS file system, analyze it, plot it, etc.

We can run this exact scenario for real on our local machine. Make sure you've
run `frankenpaxosJMV/assembly` in sbt and then run the following from the
frankenpaxos directory where `~/.ssh/id_rsa` is the private SSH key you can use
to run `ssh localhost`:

```bash
python -m benchmarks.unreplicated.smoke \
    --cluster benchmarks/unreplicated/local_cluster.json \
    -i ~/.ssh/id_rsa
```

If everything runs correctly, you should see some output that looks something
like this:

```
Running suite in /tmp/2021-03-24_19:54:20.094883_WCHQOJFXVA_unreplicated_smoke.
[001/001; 100.0%] 0:00:08 / 0:00:08 + 0:00:00? {'num_client_procs': 1, 'num_clients_per_proc': 1, 'latency.median_ms': '0.096862', 'start_throughput_1s.p90': '17237.0'}
```

If you look inside of `local_cluster.json`, you see that we're running a
benchmark where the server and all the clients run on localhost. And rather
than writing to some EFS mount, the processes all write to a directory in
`/tmp`. You can pass the `--help` flag to the script to see the other flags.
Notice, for example, that the script used the default location of the JAR file
(i.e.
`frankenpaxos/jvm/target/scala-2.12/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar`).

All of the benchmarks in this directory work like this. You provide a list of
IP addresses, and the script takes care of running the right commands with the
right flags.

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
