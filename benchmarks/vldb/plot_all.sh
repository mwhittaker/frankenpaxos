#! /usr/bin/env bash

set -euo pipefail

run_script() {
    echo "$1"
    echo "$1" | sed -e 's/./=/g'
    bash "$1"
    echo ""
}

main() {
    local -r d="$(dirname $0)"
    run_script "$d/ablation/plot.sh"
    run_script "$d/chaos/plot.sh"
    run_script "$d/horizontal_leader_failure/plot.sh"
    run_script "$d/horizontal_leader_reconfiguration/plot.sh"
    run_script "$d/leader_failure/plot.sh"
    run_script "$d/leader_reconfiguration/plot.sh"
    run_script "$d/lt/plot.sh"
    run_script "$d/matchmaker_reconfiguration/plot.sh"
    run_script "$d/more_clients_leader_reconfiguration/plot.sh"
    run_script "$d/non_thrifty_leader_reconfiguration/plot.sh"
    run_script "$d/non_thrifty_lt/plot.sh"
}

main "$@"
