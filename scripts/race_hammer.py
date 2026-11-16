"""Manual end-to-end race hammer: two real processes, real sockets.

Run:  python3 scripts/race_hammer.py [rounds] [concurrency]

Each round starts a download and, concurrently with tiny random jitter,
retires the batch on the *other* instance (whose inline GC may reclaim
the shared original at any point).  Invariant verified for every request:

* status 200  -> the body is exactly ``Content-Length`` bytes and hashes
                 to the uploaded archive (a started download is complete);
* status 404  -> the batch was retired first; no broken partial 200;
* anything else, a short body, or a hash mismatch is a failure.

New downloads after a completed retirement must be 404.  This exercises
the stat/open window that the old FileResponse implementation left open,
so it fails on the pre-fix code and passes on the pinned-descriptor
implementation.  Exits non-zero on failure.
"""
from __future__ import annotations

import hashlib
import io
import os
import random
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_healthy(url: str, proc: subprocess.Popen) -> None:
    for _ in range(200):
        if proc.poll() is not None:
            raise SystemExit(f"server died: {proc.stderr.read()}")
        try:
            with urllib.request.urlopen(f"{url}/healthz", timeout=1) as r:
                if r.status == 200:
                    return
        except OSError:
            time.sleep(0.05)
    raise SystemExit(f"{url} never became healthy")


def http(method: str, url: str, body: bytes | None = None):
    req = urllib.request.Request(url, data=body, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


def upload(url: str, batch: str, blob: bytes) -> int:
    boundary = "----hammerboundary"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{batch}.zip"\r\n'
        "Content-Type: application/zip\r\n\r\n"
    ).encode() + blob + f"\r\n--{boundary}--\r\n".encode()
    req = urllib.request.Request(
        f"{url}/batches/{batch}/archive", data=body, method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status


def read_headers(sock: socket.socket) -> tuple[int, dict[str, str], bytes]:
    raw = b""
    while b"\r\n\r\n" not in raw:
        chunk = sock.recv(65536)
        if not chunk:
            raise AssertionError("closed before headers")
        raw += chunk
    head, rest = raw.split(b"\r\n\r\n", 1)
    lines = head.decode("latin-1").split("\r\n")
    status = int(lines[0].split()[1])
    headers = {}
    for line in lines[1:]:
        k, _, v = line.partition(":")
        headers[k.strip().lower()] = v.strip()
    return status, headers, rest


class Tally:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.failures: list[str] = []
        self.complete = 0
        self.too_late = 0

    def fail(self, message: str) -> None:
        with self.lock:
            self.failures.append(message)
            print("   FAIL " + message, flush=True)

    def ok(self, kind: str) -> None:
        with self.lock:
            if kind == "complete":
                self.complete += 1
            else:
                self.too_late += 1


def one_round(index: int, port: int, url_other: str, tally: Tally) -> None:
    batch = f"H{index:05d}"
    payload = os.urandom(300_000 + index * 7919 % 1_500_000)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as zf:
        zf.writestr("reads.fastq", payload)
    blob = buf.getvalue()
    digest = hashlib.sha256(blob).hexdigest()
    if upload(f"http://127.0.0.1:{port}", batch, blob) != 200:
        tally.fail(f"{batch}: upload failed")
        return

    retire_result: list[int] = []

    def retire() -> None:
        time.sleep(random.uniform(0, 0.003))   # race the stat/open window
        status, _ = http("POST", f"{url_other}/batches/{batch}/retire")
        retire_result.append(status)

    thread = threading.Thread(target=retire)
    thread.start()

    sock = socket.socket()
    try:
        sock.connect(("127.0.0.1", port))
        sock.sendall(
            f"GET /batches/{batch}/archive HTTP/1.1\r\n"
            "Host: localhost\r\nConnection: close\r\n\r\n".encode()
        )
        status, headers, body = read_headers(sock)
        # Drain before joining the retire thread: if retirement won, the
        # server may already have answered 404 (no body).
        if status == 200:
            length = int(headers["content-length"])
            while len(body) < length:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                body += chunk
        else:
            length = 0
    except OSError as exc:
        tally.fail(f"{batch}: socket error {exc}")
        return
    finally:
        sock.close()
        thread.join(timeout=30)

    if retire_result != [200]:
        tally.fail(f"{batch}: retire status {retire_result}")
        return

    if status == 200:
        if len(body) != length:
            tally.fail(f"{batch}: short body {len(body)} != {length}")
        elif hashlib.sha256(body).hexdigest() != digest:
            tally.fail(f"{batch}: body hash mismatch")
        else:
            tally.ok("complete")
    elif status == 404:
        tally.ok("too_late")
    else:
        tally.fail(f"{batch}: unexpected status {status}")

    # A fresh download can never see the retired number again.
    new_status, _ = http("GET", f"http://127.0.0.1:{port}/batches/{batch}/archive")
    if new_status != 404:
        tally.fail(f"{batch}: new download status {new_status}")


def main() -> int:
    rounds = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    workers = int(sys.argv[2]) if len(sys.argv) > 2 else 4
    data_dir = Path(tempfile.mkdtemp(prefix="hammer-data-"))
    ports = [free_port(), free_port()]
    env_base = dict(
        os.environ,
        ARCHIVE_DATA_DIR=str(data_dir),
        PYTHONPATH=os.pathsep.join(
            [str(ROOT), os.environ.get("PYTHONPATH", "")]
        ),
    )
    procs = []
    for port in ports:
        env = dict(env_base, ARCHIVE_TEST_PORT=str(port),
                   ENABLE_SHUTDOWN_ENDPOINT="1")
        procs.append(subprocess.Popen(
            [sys.executable, str(ROOT / "scripts" / "serve.py")],
            cwd=str(ROOT), env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
        ))
    urls = [f"http://127.0.0.1:{p}" for p in ports]
    tally = Tally()
    try:
        for url, proc in zip(urls, procs):
            wait_healthy(url, proc)

        next_index = 0
        index_lock = threading.Lock()

        def worker(worker_id: int) -> None:
            nonlocal next_index
            while True:
                with index_lock:
                    if next_index >= rounds:
                        return
                    index = next_index
                    next_index += 1
                # Download from A, retire on B (and vice versa) so both
                # processes take part in every interleaving.
                if index % 2 == 0:
                    port, other = ports[0], urls[1]
                else:
                    port, other = ports[1], urls[0]
                one_round(index, port, other, tally)

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(workers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        summary = http("GET", f"{urls[0]}/storage/summary")[1]
        print("summary:", summary.decode())
        print(f"complete downloads: {tally.complete}, "
              f"retired before headers: {tally.too_late}, "
              f"failures: {len(tally.failures)}")
        return 0 if not tally.failures else 1
    finally:
        for url in urls:
            try:
                http("POST", f"{url}/internal/shutdown")
            except OSError:
                pass
        for proc in procs:
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                try:
                    proc.kill()
                except PermissionError:
                    pass
        for proc in procs:
            err = proc.stderr.read() if proc.stderr else ""
            if "Traceback" in err:
                print("SERVER TRACEBACK:\n" + err[-3000:], file=sys.stderr)
                tally.fail("server traceback")


if __name__ == "__main__":
    sys.exit(main())
