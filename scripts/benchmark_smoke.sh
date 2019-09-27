#! /usr/bin/env bash

set -euo pipefail

main() {
    for protocol in fastmultipaxos epaxos simplebpaxos superbpaxos \
                    simplegcbpaxos unanimousbpaxos; do
        echo "Running $protocol."
        python -m "benchmarks.${protocol}.smoke" \
            -m \
            --cluster "benchmarks/${protocol}/local_cluster.json" \
            -i ~/.ssh/id_rsa
    done
}

main "$@"
