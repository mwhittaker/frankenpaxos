#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb.leader_reconfiguration.plot \
        --sample_every "100" \
        --drop_head "10" \
        --drop_tail "10" \
        --f1n1 <(gunzip -c "$d/f1n1.csv.gz") \
        --f1n4 <(gunzip -c "$d/f1n4.csv.gz") \
        --f1n8 <(gunzip -c "$d/f1n8.csv.gz") \
        --f2n1 <(gunzip -c "$d/f2n1.csv.gz") \
        --f2n4 <(gunzip -c "$d/f2n4.csv.gz") \
        --f2n8 <(gunzip -c "$d/f2n8.csv.gz") \
        --output_f1 "$d/../output/leader_reconfiguration_f1.pdf" \
        --output_f2 "$d/../output/leader_reconfiguration_f2.pdf" \
        --output_violin_throughput "$d/../output/leader_reconfiguration_violin_throughput.pdf" \
        --output_violin_latency "$d/../output/leader_reconfiguration_violin_latency.pdf"
}

main "$@"
