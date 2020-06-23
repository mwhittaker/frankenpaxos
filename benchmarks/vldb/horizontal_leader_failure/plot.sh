#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb.horizontal_leader_failure.plot \
        --sample_every "100" \
        --drop_head "12" \
        --drop_tail "3" \
        --f1n1 <(gunzip -c "$d/f=1_n=1.csv.gz") \
        --f1n4 <(gunzip -c "$d/f=1_n=4.csv.gz") \
        --f1n8 <(gunzip -c "$d/f=1_n=8.csv.gz") \
        --output "$d/../output/horizontal_leader_failure.pdf"
}

main "$@"
