#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb.ablation.plot \
        --sample_every "100" \
        --drop_head "19" \
        --drop_tail "17" \
        --baseline <(gunzip -c "$d/baseline.csv.gz") \
        --gc <(gunzip -c "$d/gc.csv.gz") \
        --phase1 <(gunzip -c "$d/phase1.csv.gz") \
        --matchmaking <(gunzip -c "$d/matchmaking.csv.gz") \
        --output "$d/../output/ablation.pdf"
}

main "$@"
