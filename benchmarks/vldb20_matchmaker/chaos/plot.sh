#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb20_matchmaker.chaos.plot \
        --sample_every "100" \
        --drop_head "10" \
        --drop_tail "10" \
        --n1 <(gunzip -c "$d/n1.csv.gz") \
        --n4 <(gunzip -c "$d/n4.csv.gz") \
        --n8 <(gunzip -c "$d/n8.csv.gz") \
        --output "$d/../output/chaos.pdf"
}

main "$@"
