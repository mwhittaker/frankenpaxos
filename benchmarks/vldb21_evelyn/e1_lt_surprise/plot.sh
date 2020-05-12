#! /usr/bin/env bash

set -euo pipefail

main() {
    python -m benchmarks.nsdi.fig1_lt_plot \
        --multipaxos_results benchmarks/nsdi/fig1_lt_multipaxos_results.csv \
        --epaxos_results benchmarks/nsdi/fig1_lt_epaxos_results.csv \
        --bpaxos_results benchmarks/nsdi/fig1_lt_simplebpaxos_results.csv
}

main "$@"
