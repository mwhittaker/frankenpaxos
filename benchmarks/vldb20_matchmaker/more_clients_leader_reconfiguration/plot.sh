#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb20_matchmaker.more_clients_leader_reconfiguration.plot \
        --sample_every "100" \
        --drop_head "10" \
        --drop_tail "10" \
        --f1n100 <(gunzip -c "$d/f=1_n=100.csv.gz") \
        --output "$d/../output/more_clients_leader_reconfiguration.pdf"
}

main "$@"
