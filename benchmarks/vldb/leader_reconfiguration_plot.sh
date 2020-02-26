#! /usr/bin/env bash

set -euo pipefail

main() {
    python -m benchmarks.vldb.leader_reconfiguration_plot \
        --sample_every "100" \
        --drop_head "10" \
        --drop_tail "10" \
        --f1n1 <(gunzip -c benchmarks/vldb/leader_reconfiguration_f=1_n=1.csv.gz) \
        --f1n4 <(gunzip -c benchmarks/vldb/leader_reconfiguration_f=1_n=4.csv.gz) \
        --f1n8 <(gunzip -c benchmarks/vldb/leader_reconfiguration_f=1_n=8.csv.gz) \
        --f2n1 <(gunzip -c benchmarks/vldb/leader_reconfiguration_f=2_n=1.csv.gz) \
        --f2n4 <(gunzip -c benchmarks/vldb/leader_reconfiguration_f=2_n=4.csv.gz) \
        --f2n8 <(gunzip -c benchmarks/vldb/leader_reconfiguration_f=2_n=8.csv.gz) \
        --output_f1 "benchmarks/vldb/vldb_leader_reconfiguration_f=1.pdf" \
        --output_f2 "benchmarks/vldb/vldb_leader_reconfiguration_f=2.pdf" \
        --output_violin_throughput "benchmarks/vldb/vldb_leader_reconfiguration_violin_throughput.pdf" \
        --output_violin_latency "benchmarks/vldb/vldb_leader_reconfiguration_violin_latency.pdf"
}

main "$@"
