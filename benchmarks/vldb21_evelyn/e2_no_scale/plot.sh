#! /usr/bin/env bash

set -euo pipefail

main() {
    python -m benchmarks.vldb21_evelyn.e2_no_scale.plot \
        --results benchmarks/vldb21_evelyn/e2_no_scale/results.csv \
        --output benchmarks/vldb21_evelyn/e2_no_scale.pdf
}

main "$@"
