#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb.horizontal_leader_reconfiguration.plot \
        --sample_every "100" \
        --drop_head "10" \
        --drop_tail "5" \
        --f1n1 <(gunzip -c "$d/f=1_n=1.csv.gz") \
        --f1n4 <(gunzip -c "$d/f=1_n=4.csv.gz") \
        --f1n8 <(gunzip -c "$d/f=1_n=8.csv.gz") \
        --output "$d/../output/horizontal_leader_reconfiguration.pdf" \
        --output_violin_throughput "$d/../output/horizontal_leader_reconfiguration_violin_throughput.pdf" \
        --output_violin_latency "$d/../output/horizontal_leader_reconfiguration_violin_latency.pdf"
}

main "$@"
