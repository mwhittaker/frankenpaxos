#! /usr/bin/env bash

set -euo pipefail

main() {
    yapf \
        --recursive benchmarks \
        --in-place \
        --parallel \
        --verbose
}

main "$@"
