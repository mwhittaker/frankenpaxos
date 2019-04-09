#! /usr/bin/env bash

set -euo pipefail

main() {
    pylint benchmarks --errors-only
}

main "$@"
