"""relay.py -- raw TCP pipe 127.0.0.1:<listen> -> 127.0.0.1:<target>.

Exists so the isolated CLI can reach the operator's v2ray listener without using
its port number and without any system-wide proxy setting. The CLI speaks plain
HTTP CONNECT to the listen port; the v2ray listener terminates it.
"""
import socket, sys, threading

listen, target = int(sys.argv[1]), int(sys.argv[2])


def pump(a, b):
    try:
        while True:
            d = a.recv(32768)
            if not d:
                break
            b.sendall(d)
    except OSError:
        pass
    finally:
        for s in (a, b):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def main():
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", listen))
    srv.listen(64)
    print(f"relay {listen} -> {target}", flush=True)
    while True:
        c, _ = srv.accept()
        try:
            u = socket.create_connection(("127.0.0.1", target), timeout=10)
            u.settimeout(None)
        except OSError:
            c.close()
            continue
        threading.Thread(target=pump, args=(c, u), daemon=True).start()
        threading.Thread(target=pump, args=(u, c), daemon=True).start()


if __name__ == "__main__":
    main()
