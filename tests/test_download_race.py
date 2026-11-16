"""Download vs. retirement/reclamation race, and Range/disconnect behavior.

These tests drive the real ASGI app with hand-controlled ``send``/``receive``
so the interleaving is deterministic rather than timing-dependent:

* headers are flushed to the client,
* the batch is retired and GC reclaims the original (on the same instance
  or in a second process sharing the volume),
* only then is the response body allowed to flow.

The invariant under test: once ``200`` + ``Content-Length`` reach the
client, every promised byte is delivered from the pinned descriptor, even
though the directory entry is gone.  New downloads after retirement are
404, and a disconnect releases the descriptor.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import socket
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

from conftest import make_zip, upload

ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "scripts" / "serve.py"

import site  # noqa: E402

_USER_SITE = site.getusersitepackages()


def _scope(method: str, path: str, headers: dict[str, str] | None = None) -> dict:
    return {
        "type": "http",
        "http_version": "1.1",
        "method": method,
        "path": path,
        "raw_path": path.encode(),
        "root_path": "",
        "scheme": "http",
        "query_string": b"",
        "headers": [(k.lower().encode(), v.encode())
                    for k, v in (headers or {}).items()]
        + [(b"host", b"testserver")],
        "client": ("testclient", 50000),
        "server": ("testserver", 80),
        "state": {},
    }


class _Exchange:
    """Drive one ASGI request while pausing at chosen checkpoints."""

    def __init__(self, pause_bodies: bool = False) -> None:
        self.status: int | None = None
        self.headers: dict[str, str] = {}
        self.body = bytearray()
        self.started = asyncio.Event()
        self.resume = asyncio.Event()
        self.disconnect = asyncio.Event()
        self.first_body = asyncio.Event()
        self.allow_more = asyncio.Event()
        self.pause_bodies = pause_bodies

    async def send(self, message) -> None:
        if message["type"] == "http.response.start":
            self.status = message["status"]
            self.headers = {
                k.decode().lower(): v.decode() for k, v in message["headers"]
            }
            self.started.set()
            await self.resume.wait()
        elif message["type"] == "http.response.body":
            chunk = message.get("body", b"")
            if chunk:
                self.body.extend(chunk)
                if self.pause_bodies:
                    self.first_body.set()
                    await self.allow_more.wait()

    async def receive(self):
        await self.disconnect.wait()
        return {"type": "http.disconnect"}


def _retire_and_gc_sql(data_dir: Path, batch_id: str) -> None:
    """Retire via the same states the API writes, then run GC in-process."""
    from app import storage

    conn = sqlite3.connect(data_dir / "archives.db")
    conn.execute(
        "UPDATE batches SET status='retired', "
        "retired_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE batch_id=?",
        (batch_id,),
    )
    conn.commit()
    conn.close()
    storage.run_gc()


def test_headers_then_retirement_still_yields_full_body(client, data_dir):
    """The exact reported race: 200 sent, batch retired, body must be whole."""
    from app.api import app

    body = make_zip({"reads.fastq": b"ACGT" * 40_000})
    assert upload(client, "RACE1", body).status_code == 200
    digest = hashlib.sha256(body).hexdigest()

    async def scenario():
        ex = _Exchange()
        scope = _scope("GET", "/batches/RACE1/archive")
        task = asyncio.create_task(app(scope, ex.receive, ex.send))
        await ex.started.wait()
        assert ex.status == 200
        assert ex.headers["content-length"] == str(len(body))
        assert ex.body == b""                     # parked before any body

        # Other instance's work is modelled in-process: retired + reclaimed.
        await asyncio.to_thread(_retire_and_gc_sql, data_dir, "RACE1")
        assert not (data_dir / "blobs" / digest).exists()
        assert not (data_dir / "quarantine" / digest).exists()

        ex.resume.set()
        await task
        return ex

    ex = asyncio.run(scenario())
    assert ex.status == 200
    assert bytes(ex.body) == body
    assert hashlib.sha256(ex.body).hexdigest() == digest

    # The retired number is dead for new requests.
    assert client.get("/batches/RACE1/archive").status_code == 404
    summary = client.get("/storage/summary").json()
    assert summary["blob_rows"] == summary["blob_files_on_disk"] == 0


def test_retirement_between_lookup_and_open_is_404_not_broken_200(
    client, data_dir, monkeypatch
):
    """If GC wins before the descriptor opens, answer 404 before headers."""
    from app import storage

    body = make_zip({"late": b"gone-before-open"})
    assert upload(client, "RACE2", body).status_code == 200

    real_open = storage.open_original

    def open_after_reclaim(sha256):
        _retire_and_gc_sql(data_dir, "RACE2")
        return real_open(sha256)

    monkeypatch.setattr(storage, "open_original", open_after_reclaim)
    r = client.get("/batches/RACE2/archive")
    assert r.status_code == 404
    assert "unavailable" in r.json()["detail"]


def test_blob_still_in_quarantine_can_be_downloaded(client, data_dir):
    """A live reference racing a sweep must still yield the original.

    GC moves bytes to quarantine between its row flip and reference
    re-check.  If a new active batch references the hash in that window,
    the download must find the bytes in quarantine (same inode) rather
    than 404.
    """
    body = make_zip({"rescue": b"still-on-volume"})
    assert upload(client, "Q1", body).status_code == 200
    digest = hashlib.sha256(body).hexdigest()

    # Model the sweep's phase-one state: bytes in quarantine, row deleting.
    conn = sqlite3.connect(data_dir / "archives.db")
    conn.execute(
        "UPDATE blobs SET status='deleting', quarantine_path=? "
        "WHERE sha256=?",
        (str(data_dir / "quarantine" / digest), digest),
    )
    conn.commit()
    conn.close()
    os.replace(data_dir / "blobs" / digest, data_dir / "quarantine" / digest)

    # Present the batch as active again (as an upload would).
    conn = sqlite3.connect(data_dir / "archives.db")
    conn.execute("UPDATE blobs SET status='ready', quarantine_path=NULL "
                 "WHERE sha256=?", (digest,))
    conn.commit()
    conn.close()

    r = client.get("/batches/Q1/archive")
    assert r.status_code == 200
    assert r.content == body
    # Download only reads; it must not mutate the quarantine layout.
    assert (data_dir / "quarantine" / digest).exists()


def test_inconsistent_original_size_is_404_not_short_200(client, data_dir):
    """A blob whose size disagrees with the ledger must not 200."""
    body = make_zip({"trunc": b"x" * 1000})
    assert upload(client, "SIZE", body).status_code == 200
    digest = hashlib.sha256(body).hexdigest()

    path = data_dir / "blobs" / digest
    path.write_bytes(body[: len(body) // 2])

    r = client.get("/batches/SIZE/archive")
    assert r.status_code == 404
    assert "inconsistent" in r.json()["detail"]


def test_new_download_404_while_old_one_is_still_streaming(client, data_dir):
    """Retire + GC complete while a pinned download is mid-flight."""
    from app.api import app

    body = make_zip({"slow": os.urandom(250_000)})
    assert upload(client, "RACE3", body).status_code == 200

    async def scenario():
        ex = _Exchange()
        task = asyncio.create_task(
            app(_scope("GET", "/batches/RACE3/archive"), ex.receive, ex.send)
        )
        await ex.started.wait()
        _retire_and_gc_sql(data_dir, "RACE3")
        # Fresh request sees a dead batch even though the first is alive.
        fresh = await _collect(app, "GET", "/batches/RACE3/archive")
        assert fresh[0] == 404
        ex.resume.set()
        await task
        return ex

    ex = asyncio.run(scenario())
    assert bytes(ex.body) == body
    assert client.get("/batches/RACE3/archive").status_code == 404


# --------------------------------------------------------------------------- #
# Range behavior (must match the previous FileResponse semantics)
# --------------------------------------------------------------------------- #

@pytest.fixture
def ranged(client):
    body = make_zip({"range.bin": bytes(range(256)) * 8})
    assert upload(client, "RANGE", body).status_code == 200
    return body


def test_single_range(client, ranged):
    r = client.get("/batches/RANGE/archive",
                   headers={"Range": "bytes=10-99"})
    assert r.status_code == 206
    assert r.headers["content-range"] == f"bytes 10-99/{len(ranged)}"
    assert r.headers["content-length"] == "90"
    assert r.content == ranged[10:100]


def test_suffix_and_open_ended_ranges(client, ranged):
    r = client.get("/batches/RANGE/archive", headers={"Range": "bytes=-16"})
    assert r.status_code == 206
    assert r.content == ranged[-16:]

    r = client.get("/batches/RANGE/archive",
                   headers={"Range": f"bytes={len(ranged) - 7}-"})
    assert r.status_code == 206
    assert r.content == ranged[-7:]


def test_multiple_ranges_and_declared_length(client, ranged):
    r = client.get("/batches/RANGE/archive",
                   headers={"Range": "bytes=0-9,20-29"})
    assert r.status_code == 206
    assert r.headers["content-range"].startswith("multipart/byteranges")
    # httpx assembles the raw multipart body; its length must be what the
    # response advertised, and both slices must be present verbatim.
    assert len(r.content) == int(r.headers["content-length"])
    assert ranged[0:10] in r.content
    assert ranged[20:30] in r.content


def test_unsatisfiable_and_malformed_ranges(client, ranged):
    r = client.get("/batches/RANGE/archive",
                   headers={"Range": f"bytes={len(ranged)}-"})
    assert r.status_code == 416
    assert r.headers["content-range"] == f"*/{len(ranged)}"

    r = client.get("/batches/RANGE/archive", headers={"Range": "items=0-1"})
    assert r.status_code == 400


def test_if_range_matching_etag_uses_range(client, ranged):
    head = client.get("/batches/RANGE/archive")
    etag = head.headers["etag"]

    r = client.get("/batches/RANGE/archive",
                   headers={"Range": "bytes=0-3", "If-Range": etag})
    assert r.status_code == 206
    assert r.content == ranged[:4]

    # A stale validator forces the full 200 body.
    r = client.get("/batches/RANGE/archive",
                   headers={"Range": "bytes=0-3", "If-Range": '"stale"'})
    assert r.status_code == 200
    assert r.content == ranged


def test_head_response_advertises_full_length(client, ranged, data_dir):
    """HEAD is handled (headers only) by the download response itself.

    FastAPI's route table only exposes GET for this endpoint (unchanged
    from before), so HEAD is exercised at the ASGI response level.
    """
    from app.download import ArchiveDownloadResponse

    digest = hashlib.sha256(ranged).hexdigest()
    fd = os.open(str(data_dir / "blobs" / digest), os.O_RDONLY)
    st = os.fstat(fd)
    response = ArchiveDownloadResponse(fd, st, filename="RANGE.zip")

    async def scenario():
        ex = _Exchange()
        ex.resume.set()
        await response(_scope("HEAD", "/batches/RANGE/archive"),
                       ex.receive, ex.send)
        return ex

    ex = asyncio.run(scenario())
    assert ex.status == 200
    assert ex.headers["content-length"] == str(len(ranged))
    assert bytes(ex.body) == b""
    assert response.closed is True


# --------------------------------------------------------------------------- #
# Disconnect handling
# --------------------------------------------------------------------------- #

def test_client_disconnect_closes_descriptor_and_stops_reading(
    client, data_dir
):
    """A dropped client must not keep a reclaimed original pinned."""
    from app.api import app
    from app.download import ArchiveDownloadResponse

    body = make_zip({"big": os.urandom(400_000)})
    assert upload(client, "DISC", body).status_code == 200
    digest = hashlib.sha256(body).hexdigest()

    captured: list[ArchiveDownloadResponse] = []
    original_init = ArchiveDownloadResponse.__init__

    def capture(self, *a, **kw):
        original_init(self, *a, **kw)
        captured.append(self)

    ArchiveDownloadResponse.__init__ = capture
    try:
        async def scenario():
            ex = _Exchange(pause_bodies=True)
            task = asyncio.create_task(
                app(_scope("GET", "/batches/DISC/archive"),
                    ex.receive, ex.send)
            )
            await ex.started.wait()
            # Release the headers so body streaming starts, then let the
            # client vanish before the transfer completes.
            ex.resume.set()
            await asyncio.wait_for(ex.first_body.wait(), timeout=10)
            _retire_and_gc_sql(data_dir, "DISC")
            assert not (data_dir / "blobs" / digest).exists()
            ex.disconnect.set()
            await asyncio.wait_for(task, timeout=10)
            return ex

        ex = asyncio.run(scenario())
    finally:
        ArchiveDownloadResponse.__init__ = original_init

    assert len(captured) == 1
    assert captured[0].closed is True
    # Partial transfer is allowed; what is forbidden is holding the fd.
    assert bytes(ex.body) != body
    assert client.get("/batches/DISC/archive").status_code == 404


def test_normal_completion_closes_descriptor(client, data_dir):
    from app.api import app
    from app.download import ArchiveDownloadResponse

    body = make_zip({"ok": b"complete"})
    assert upload(client, "DONE", body).status_code == 200

    captured = []
    original_init = ArchiveDownloadResponse.__init__

    def capture(self, *a, **kw):
        original_init(self, *a, **kw)
        captured.append(self)

    ArchiveDownloadResponse.__init__ = capture
    try:
        async def scenario():
            ex = _Exchange()
            ex.resume.set()  # no pause
            await app(_scope("GET", "/batches/DONE/archive"),
                      ex.receive, ex.send)
            return ex

        ex = asyncio.run(scenario())
    finally:
        ArchiveDownloadResponse.__init__ = original_init

    assert bytes(ex.body) == body
    assert captured[0].closed is True


def test_descriptor_released_on_real_http_disconnect(data_dir, monkeypatch):
    """A real dropped TCP connection stops streaming and releases the fd.

    A uvicorn server runs in this process (thread), so the test can both
    park the server deterministically (one blocking pread) and inspect the
    process descriptor table.  The client reads the headers plus one
    chunk, then closes the socket.  The server must stop reading instead
    of draining the whole reclaimed original, and close the descriptor.
    """
    import threading

    import uvicorn

    from app import download as download_module

    blocked = threading.Event()
    release = threading.Event()
    reads: list[int] = []
    real_pread = download_module._pread

    def blocking_pread(fd, length, offset):
        if offset > 0 and not release.is_set():
            blocked.set()
            release.wait(10)
        data = real_pread(fd, length, offset)
        reads.append(len(data))
        return data

    monkeypatch.setattr(download_module, "_pread", blocking_pread)

    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(
        "app.api:app", host="127.0.0.1", port=port, log_level="warning"
    ))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 20
        while not server.started and time.monotonic() < deadline:
            time.sleep(0.05)
        assert server.started, "in-process uvicorn did not start"
        base = f"http://127.0.0.1:{port}"

        body = make_zip({"drop": os.urandom(1_500_000)})
        digest = hashlib.sha256(body).hexdigest()
        with httpx.Client(timeout=60) as c:
            assert c.post(f"{base}/batches/DROPFDS/archive",
                          files={"file": ("d.zip", body, "application/zip")}
                          ).status_code == 200

        before = _fds_referencing_digest(digest)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(("127.0.0.1", port))
            sock.sendall(
                b"GET /batches/DROPFDS/archive HTTP/1.1\r\n"
                b"Host: localhost\r\nConnection: close\r\n\r\n"
            )
            status, headers, rest = _read_http_headers(sock)
            assert status == 200
            assert int(headers["content-length"]) == len(body)
            # The server parks on its second read: one chunk emitted.
            assert blocked.wait(15), "server never reached the parked read"
            assert _fds_referencing_digest(digest) > before
        finally:
            sock.close()                     # client vanishes mid-transfer

        # Let the disconnect propagate, then unpark the worker thread.
        time.sleep(0.3)
        release.set()

        # Retire + reclaim while the transfer is being abandoned.
        with httpx.Client(timeout=30) as c:
            assert c.post(f"{base}/batches/DROPFDS/retire").status_code == 200
        assert not (data_dir / "blobs" / digest).exists()

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if _fds_referencing_digest(digest) == before:
                break
            time.sleep(0.05)
        assert _fds_referencing_digest(digest) == before
        # It stopped early: without the disconnect watcher the loop would
        # have drained every chunk of the file (send is a no-op once
        # uvicorn marks the connection gone).
        assert sum(reads) < len(body), reads
    finally:
        release.set()
        server.should_exit = True
        thread.join(timeout=15)


def _fds_referencing_digest(digest: str) -> int:
    """Count this process's open descriptors naming ``digest``.

    After GC unlinks the original the symlink reads ``.../<digest>
    (deleted)``; until our response closes its fd that entry remains, so
    this is a direct probe of "is a reclaimed original still pinned?".
    """
    count = 0
    for entry in os.listdir("/proc/self/fd"):
        try:
            target = os.readlink(f"/proc/self/fd/{entry}")
        except OSError:
            continue
        if digest in target:
            count += 1
    return count


# --------------------------------------------------------------------------- #
# Two real OS processes sharing one volume
# --------------------------------------------------------------------------- #

def _free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_healthy(url: str, proc: subprocess.Popen) -> None:
    for _ in range(100):
        if proc.poll() is not None:
            raise RuntimeError(f"server exited early: {proc.stderr.read()!r}")
        try:
            if httpx.get(f"{url}/healthz", timeout=1).status_code == 200:
                return
        except httpx.TransportError:
            time.sleep(0.1)
    raise RuntimeError(f"server at {url} never became healthy")


@pytest.fixture
def two_instances(data_dir):
    ports = [_free_port(), _free_port()]
    env_base = dict(os.environ, ARCHIVE_DATA_DIR=str(data_dir))
    procs = []
    for port in ports:
        env = dict(
            env_base,
            ARCHIVE_TEST_PORT=str(port),
            ENABLE_SHUTDOWN_ENDPOINT="1",
            PYTHONPATH=os.pathsep.join([str(ROOT), _USER_SITE]),
        )
        procs.append(subprocess.Popen(
            [sys.executable, str(SERVER)],
            cwd=str(ROOT),
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        ))
    urls = [f"http://127.0.0.1:{p}" for p in ports]
    try:
        for url, proc in zip(urls, procs):
            _wait_healthy(url, proc)
        yield urls
    finally:
        for url in urls:
            try:
                httpx.post(f"{url}/internal/shutdown", timeout=5)
            except httpx.HTTPError:
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
                raise AssertionError("server traceback:\n" + err[-4000:])


def test_cross_instance_retire_mid_download_keeps_body_intact(two_instances):
    """A on instance 1 streams; B on instance 2 retires and reclaims."""
    url_a, url_b = two_instances
    body = make_zip({"cross": os.urandom(500_000)})
    digest = hashlib.sha256(body).hexdigest()

    with httpx.Client(timeout=10) as c:
        r = c.post(f"{url_a}/batches/CROSS/archive",
                   files={"file": ("c.zip", body, "application/zip")})
        assert r.status_code == 200

    with httpx.Client(timeout=60) as c:
        with c.stream("GET", f"{url_a}/batches/CROSS/archive") as r:
            assert r.status_code == 200
            assert int(r.headers["content-length"]) == len(body)
            stream = r.iter_bytes(2048)
            chunks = [next(stream)]        # headers + first bytes out
            assert chunks[0]

            # Instance B retires and reclaims while A's descriptor is open.
            rr = httpx.post(f"{url_b}/batches/CROSS/retire", timeout=20)
            assert rr.status_code == 200
            chunks.extend(stream)

    got = b"".join(chunks)
    assert len(got) == len(body)
    assert hashlib.sha256(got).hexdigest() == digest

    assert httpx.get(f"{url_b}/batches/CROSS/archive",
                     timeout=10).status_code == 404
    summary = httpx.get(f"{url_b}/storage/summary", timeout=10).json()
    assert summary["blob_rows"] == summary["blob_files_on_disk"] == 0


def test_cross_instance_range_download_survives_reclamation(two_instances):
    url_a, url_b = two_instances
    body = make_zip({"chunk": os.urandom(200_000)})
    with httpx.Client(timeout=10) as c:
        assert c.post(f"{url_a}/batches/XRANGE/archive",
                      files={"file": ("x.zip", body, "application/zip")}
                      ).status_code == 200

    with httpx.Client(timeout=60) as c:
        with c.stream("GET", f"{url_a}/batches/XRANGE/archive",
                      headers={"Range": "bytes=1000-199999"}) as r:
            assert r.status_code == 206
            assert r.headers["content-length"] == str(199_000)
            stream = r.iter_bytes(1024)
            got = bytearray(next(stream))
            httpx.post(f"{url_b}/batches/XRANGE/retire", timeout=20)
            for chunk in stream:
                got.extend(chunk)

    assert bytes(got) == body[1000:200_000]


def _read_http_headers(sock) -> tuple[int, dict[str, str], bytes]:
    raw = b""
    while b"\r\n\r\n" not in raw:
        chunk = sock.recv(65536)
        if not chunk:
            raise AssertionError("connection closed before headers")
        raw += chunk
    head, rest = raw.split(b"\r\n\r\n", 1)
    lines = head.decode("latin-1").split("\r\n")
    status = int(lines[0].split()[1])
    headers = {}
    for line in lines[1:]:
        key, _, value = line.partition(":")
        headers[key.strip().lower()] = value.strip()
    return status, headers, rest


def test_http_level_headers_only_then_retire_on_other_instance(two_instances,
                                                                data_dir):
    """The reported interleaving at the real HTTP layer.

    1. raw TCP client sends GET to instance A and reads *only* the response
       head (200 + Content-Length), then stops reading;
    2. instance B retires the batch and its inline GC removes the original;
    3. the client drains the socket and must receive exactly the advertised
       number of bytes - the complete archive.
    """
    from urllib.parse import urlparse

    url_a, url_b = two_instances
    body = make_zip({"payload.bin": os.urandom(1_800_000)})
    digest = hashlib.sha256(body).hexdigest()
    with httpx.Client(timeout=30) as c:
        assert c.post(f"{url_a}/batches/RAWSOCK/archive",
                      files={"file": ("r.zip", body, "application/zip")}
                      ).status_code == 200

    parsed = urlparse(url_a)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Tiny receive window so the server parks mid-body after headers.
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    try:
        sock.connect((parsed.hostname, parsed.port))
        sock.sendall(
            b"GET /batches/RAWSOCK/archive HTTP/1.1\r\n"
            b"Host: localhost\r\nConnection: close\r\n\r\n"
        )
        status, headers, rest = _read_http_headers(sock)
        assert status == 200
        length = int(headers["content-length"])
        assert length == len(body)
        assert headers["accept-ranges"] == "bytes"

        # Retire on the other instance while A is still parked.
        rr = httpx.post(f"{url_b}/batches/RAWSOCK/retire", timeout=30)
        assert rr.status_code == 200
        summary = httpx.get(f"{url_b}/storage/summary", timeout=10).json()
        assert summary["blob_rows"] == 0
        assert summary["blob_files_on_disk"] == 0
        assert not (data_dir / "blobs" / digest).exists()

        # New download is already 404.
        assert httpx.get(f"{url_b}/batches/RAWSOCK/archive",
                         timeout=10).status_code == 404

        # Drain the parked response; it must be complete and exact.
        payload = bytearray(rest)
        while len(payload) < length:
            chunk = sock.recv(65536)
            if not chunk:
                break
            payload.extend(chunk)
    finally:
        sock.close()

    assert len(payload) == length
    assert hashlib.sha256(payload).hexdigest() == digest


async def _collect(app, method: str, path: str):
    ex = _Exchange()
    ex.resume.set()
    await app(_scope(method, path), ex.receive, ex.send)
    return ex.status, bytes(ex.body)
