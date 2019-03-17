# This file implements a simple client for the purposes of measuring network
# latency. First, launch the latency server like this
#
#   python latency_server.py [--host <host>] [--port <port>]
#
# Then, start the latency client using the same host and port used above:
#
#   python latency_client.py [--host <host>] [--port <port>] [-n <n>]
#
# The client opens a connection to the server and then sends `n` one byte
# messages. The client records the latency required to send the message and
# receive a response (measured in microseconds). After sending `n` messages,
# the client prints out the recorded latencies (again, in microseconds) to
# standard out.

import argparse
import socket
import time

def main(args) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((args.host, args.port))
        durations_micros = []
        for _ in range(args.n):
            start = time.time()
            s.sendall(b'.')
            s.recv(1024)
            stop = time.time()
            durations_micros.append((stop - start) * 1000 * 1000)
        for d in durations_micros:
            print(d)

def arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--host',
        type=str,
        default='localhost',
        help='Server host'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=8000,
        help='Server port'
    )
    parser.add_argument(
        '-n',
        type=int,
        default=1000,
        help='Number of latency measurements'
    )
    return parser

if __name__ == '__main__':
    main(arg_parser().parse_args())
