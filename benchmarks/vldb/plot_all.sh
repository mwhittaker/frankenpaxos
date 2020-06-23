#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    bash "$d/horizontal_leader_reconfiguration/plot.sh"
    bash "$d/horizontal_leader_failure/plot.sh"
}

main "$@"
