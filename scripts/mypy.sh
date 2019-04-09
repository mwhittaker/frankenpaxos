#! /usr/bin/env bash

set -euo pipefail

main() {
    mypy benchmarks --ignore-missing-imports
}

main "$@"
