#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb21_evelyn.e0_theory.plot \
        --output_dir "$d/.." \
        --alpha 100000
}

main "$@"
