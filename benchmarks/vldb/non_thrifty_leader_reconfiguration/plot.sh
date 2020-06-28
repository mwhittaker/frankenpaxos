#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb.non_thrifty_leader_reconfiguration.plot \
        --sample_every "100" \
        --drop_head "10" \
        --drop_tail "10" \
        --f1n1 <(gunzip -c "$d/f1n1.csv.gz") \
        --f1n4 <(gunzip -c "$d/f1n4.csv.gz") \
        --f1n8 <(gunzip -c "$d/f1n8.csv.gz") \
        --output "$d/../output/non_thrifty_leader_reconfiguration.pdf"
}

main "$@"
