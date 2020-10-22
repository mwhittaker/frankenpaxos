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
    run_script "$d/compartmentalized_lt/plot.sh"
    run_script "$d/batched_compartmentalized_lt/plot.sh"
    run_script "$d/ablation.sh"
}

main "$@"
