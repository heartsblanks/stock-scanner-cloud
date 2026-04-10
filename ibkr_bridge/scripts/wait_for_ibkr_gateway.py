#!/usr/bin/env python3
from __future__ import annotations

import os
import socket
import sys
import time


def main() -> int:
    host = os.getenv("IBKR_HOST", "127.0.0.1").strip() or "127.0.0.1"
    port = int(os.getenv("IBKR_PORT", "4002"))
    timeout_seconds = int(os.getenv("IBKR_GATEWAY_WAIT_TIMEOUT_SECONDS", "60"))
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(2.0)
            try:
                sock.connect((host, port))
            except OSError:
                time.sleep(2.0)
                continue
            return 0

    sys.stderr.write(f"Timed out waiting for IB Gateway on {host}:{port}\n")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
