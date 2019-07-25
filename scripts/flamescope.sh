#! /usr/bin/env bash

set -euo pipefail

usage() {
    echo "$0 -p <perf-{pid}.data> -m <perf-{pid}.map>"
}

main() {
    while getopts ":hp:m:" opt; do
        case ${opt} in
            h )
                usage "$0"
                return 0
                ;;
            p )
                perf_data="$OPTARG"
                ;;
            m )
                perf_map="$OPTARG"
                ;;
            \? )
                usage "$0"
                return 1
                ;;
            : )
                echo "$OPTARG reqiures an argument."
                usage "$0"
                return 1
                ;;
        esac
    done
    shift $((OPTIND -1))

    if [[ -z ${perf_data+dummy} ]]; then
        echo "-p is required"
        usage "$0"
        return 1
    fi

    if [[ -z ${perf_map+dummy} ]]; then
        echo "-m is required"
        usage "$0"
        return 1
    fi

    cp "$perf_map" /tmp
    sudo chown root "/tmp/$perf_map"
    sudo perf script -i "$perf_data"
    sudo rm "/tmp/$perf_map"
}

main "$@"
