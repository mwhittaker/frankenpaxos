#! /usr/bin/env bash

set -euo pipefail

main() {
    python -m benchmarks.vldb21_evelyn.e1_lt_surprise.plot \
        --results benchmarks/vldb21_evelyn/e1_lt_surprise/results.csv \
        --output_with benchmarks/vldb21_evelyn/e1_lt_with.pdf \
        --output_without benchmarks/vldb21_evelyn/e1_lt_without.pdf
}

main "$@"
