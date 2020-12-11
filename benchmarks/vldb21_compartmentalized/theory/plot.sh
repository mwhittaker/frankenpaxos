#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb21_compartmentalized.theory.plot \
        --output_dir "$d/../output/" \
        --alpha 100000
}

main "$@"
