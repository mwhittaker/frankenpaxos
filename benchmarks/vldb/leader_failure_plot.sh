#! /usr/bin/env bash

set -euo pipefail

main() {
    python -m benchmarks.vldb.leader_failure_plot \
        --sample_every "100" \
        --drop_head "12" \
        --drop_tail "3" \
        --f1n1 <(gunzip -c benchmarks/vldb/leader_failure_f=1_n=1.csv.gz) \
        --f1n4 <(gunzip -c benchmarks/vldb/leader_failure_f=1_n=4.csv.gz) \
        --f1n8 <(gunzip -c benchmarks/vldb/leader_failure_f=1_n=8.csv.gz) \
        --output_f1 "benchmarks/vldb/vldb_leader_failure_f=1.pdf"
}

main "$@"
