#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb21_compartmentalized.read_scale.plot \
        --results "$d/results.csv" \
        --output "$d/../output/read_scale.pdf"
}

main "$@"
