#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb.leader_failure.plot \
        --sample_every "100" \
        --drop_head "12" \
        --drop_tail "3" \
        --n1 <(gunzip -c "$d/n1.csv.gz") \
        --n4 <(gunzip -c "$d/n4.csv.gz") \
        --n8 <(gunzip -c "$d/n8.csv.gz") \
        --output "$d/../output/leader_failure.pdf"
}

main "$@"
