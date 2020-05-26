#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    bash "$d/e0_theory/plot.sh"
    bash "$d/e1_lt_surprise/plot.sh"
    bash "$d/e2_no_scale_replica/plot.sh"
    bash "$d/e3_no_scale_fraction/plot.sh"
    bash "$d/e4_scale_replica/plot.sh"
    bash "$d/e5_scale_load/plot.sh"
}

main "$@"
