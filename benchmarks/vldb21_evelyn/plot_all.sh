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
    run_script "$d/lt_surprise/plot.sh"
    run_script "$d/no_scale_fraction/plot.sh"
    run_script "$d/no_scale_replica/plot.sh"
    run_script "$d/scale_load/plot.sh"
    run_script "$d/scale_replica/plot.sh"
    run_script "$d/theory/plot.sh"
}

main "$@"
