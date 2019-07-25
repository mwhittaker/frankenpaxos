#! /usr/bin/env bash

set -euo pipefail

main() {
    python -m unittest discover --verbose -p '*_test.py'
}

main "$@"
