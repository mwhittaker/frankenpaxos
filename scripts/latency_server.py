# This file implements a simple echo server for the purposes of measuring
# network latency. You can launch the server like this:
#
#   python latency_server.py [--host <host>] [--port <port>]
#
# The server accepts one client connection at a time and echos back any data
# that it receives. For example, you can connect to the server using nc:
#
#   $ nc <host> <port>
#   foo
#   foo

import argparse
import socket

def handle_client(s: socket.socket) -> None:
    while True:
        data = s.recv(1024)
        if not data:
            return
        s.sendall(data)

def main(args) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((args.host, args.port))
        s.listen()
        print(f'Server listening on {args.host}:{args.port}.')

        while True:
            conn, addr = s.accept()
            print(f'Server accepted connection from {addr}.')
            with conn:
                handle_client(conn)

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
    return parser

if __name__ == '__main__':
    main(arg_parser().parse_args())
