#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb.more_clients_leader_reconfiguration_plot \
        --sample_every "100" \
        --drop_head "10" \
        --drop_tail "10" \
        --f1n100 <(gunzip -c "$d/leader_reconfiguration_f=1_n=100.csv.gz") \
        --output "$d/vldb_more_clients_leader_reconfiguration.pdf"
}

main "$@"
