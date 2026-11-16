"""Concurrency tests: two real OS processes sharing one data directory.

Two uvicorn subprocesses are spawned (independent interpreters and
SQLite connections) against the same data directory - the same topology
as the two Docker Compose replicas sharing one named volume.
"""
from __future__ import annotations

import hashlib
import os
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

from conftest import make_zip

ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "scripts" / "serve.py"

# fastapi/uvicorn live in the user site in this environment; make them
# importable in the spawned interpreters without dropping isolation of
# the data directory.
import site  # noqa: E402
_USER_SITE = site.getusersitepackages()


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_healthy(url: str, proc: subprocess.Popen) -> None:
    for _ in range(100):
        if proc.poll() is not None:
            raise RuntimeError(f"server exited early: {proc.stderr.read()!r}")
        try:
            r = httpx.get(f"{url}/healthz", timeout=1)
            if r.status_code == 200:
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
        # Cooperative HTTP shutdown: this runtime blocks os.kill, and it
        # also exercises uvicorn's graceful drain of open downloads.
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
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
        # Surface server-side errors (truncated) for test diagnostics.
        for proc in procs:
            err = proc.stderr.read() if proc.stderr else ""
            if "Traceback" in err:
                raise AssertionError(
                    "server traceback:\n" + err[-4000:]
                )


def _upload(url: str, batch_id: str, body: bytes) -> int:
    with httpx.Client(timeout=15) as c:
        r = c.post(
            f"{url}/batches/{batch_id}/archive",
            files={"file": ("b.zip", body, "application/zip")},
        )
        return r.status_code


def test_concurrent_identical_uploads_dedup_and_no_missing_files(
    two_instances, data_dir
):
    base_urls = two_instances
    body = make_zip({"lane": b"GATTACA" * 50})
    digest = hashlib.sha256(body).hexdigest()
    n = 12

    with ThreadPoolExecutor(max_workers=n) as pool:
        statuses = list(pool.map(
            lambda i: _upload(base_urls[i % 2], f"B{i:03d}", body),
            range(n),
        ))
    assert statuses == [200] * n

    # One physical original; every active batch can fetch it, even via
    # the instance that did not receive its upload.
    assert len(list((data_dir / "blobs").iterdir())) == 1
    for i in range(n):
        r = httpx.get(
            f"{base_urls[(i + 1) % 2]}/batches/B{i:03d}/archive", timeout=10
        )
        assert r.status_code == 200
        assert hashlib.sha256(r.content).hexdigest() == digest

    summary = httpx.get(f"{base_urls[0]}/storage/summary").json()
    assert summary["blob_rows"] == summary["blob_files_on_disk"] == 1
    assert summary["active_batches"] == n


def test_concurrent_retirement_across_instances(two_instances):
    base_urls = two_instances
    shared = make_zip({"shared": b"same"})
    solo = make_zip({"solo": b"other"})
    with httpx.Client(timeout=10) as c:
        c.post(f"{base_urls[0]}/batches/SHARED1/archive",
               files={"file": ("a.zip", shared, "application/zip")})
        c.post(f"{base_urls[1]}/batches/SHARED2/archive",
               files={"file": ("a.zip", shared, "application/zip")})
        c.post(f"{base_urls[0]}/batches/SOLO/archive",
               files={"file": ("a.zip", solo, "application/zip")})

    def retire(args):
        url, batch = args
        return httpx.post(f"{url}/batches/{batch}/retire",
                          timeout=15).status_code

    jobs = [
        (base_urls[0], "SHARED1"),
        (base_urls[1], "SHARED2"),
        (base_urls[1], "SOLO"),
    ]
    with ThreadPoolExecutor(max_workers=3) as pool:
        assert list(pool.map(retire, jobs)) == [200, 200, 200]

    for batch in ("SHARED1", "SHARED2", "SOLO"):
        assert httpx.get(
            f"{base_urls[0]}/batches/{batch}/archive").status_code == 404
    summary = httpx.get(f"{base_urls[0]}/storage/summary").json()
    assert summary["blob_rows"] == 0
    assert summary["blob_files_on_disk"] == 0
    assert summary["retired_batches"] == 3


def test_slow_download_finishes_and_does_not_block_other_retirements(
    two_instances
):
    base_urls = two_instances
    target_body = make_zip({"slow": b"X" * 200_000})
    other_body = make_zip({"other": b"Y" * 1000})
    with httpx.Client(timeout=10) as c:
        c.post(f"{base_urls[0]}/batches/SLOW/archive",
               files={"file": ("s.zip", target_body, "application/zip")})
        c.post(f"{base_urls[1]}/batches/OTHER/archive",
               files={"file": ("o.zip", other_body, "application/zip")})

    with httpx.Client(timeout=30) as c:
        with c.stream("GET", f"{base_urls[0]}/batches/SLOW/archive") as r:
            assert r.status_code == 200
            stream = r.iter_bytes(4096)
            first = next(stream)
            assert first

            # Transfer parked: retire a different batch on the other
            # instance; it must complete promptly (no head-of-line block).
            started = time.monotonic()
            rr = httpx.post(f"{base_urls[1]}/batches/OTHER/retire",
                            timeout=10)
            elapsed = time.monotonic() - started
            assert rr.status_code == 200
            assert elapsed < 10

            # Retire the batch being downloaded as well; GC may move the
            # file, but the open transfer must still complete.
            httpx.post(f"{base_urls[0]}/batches/SLOW/retire", timeout=10)

            full = first + b"".join(stream)
            assert hashlib.sha256(full).hexdigest() == \
                hashlib.sha256(target_body).hexdigest()

    # New downloads are dead; the old one already completed above.
    assert httpx.get(
        f"{base_urls[1]}/batches/SLOW/archive").status_code == 404


def test_mixed_workload_keeps_every_active_batch_complete(two_instances):
    """Two instances, uploads + retirements + GC + downloads interleaved.

    Invariant at the end: every active batch's original bytes exist on
    disk and the blob table matches the physical file inventory.
    """
    base_urls = two_instances
    n = 16
    bodies = {i: make_zip({f"f{i}": bytes([i % 256]) * 64}) for i in range(n)}

    def worker(i):
        url = base_urls[i % 2]
        with httpx.Client(base_url=url, timeout=20) as c:
            # Half the batches share identical content.
            body = bodies[i % 4]
            up = c.post(
                f"/batches/MIX{i:02d}/archive",
                files={"file": ("b.zip", body, "application/zip")},
            )
            assert up.status_code == 200
            # Every second worker retires its batch right away...
            if i % 2 == 0:
                assert c.post(
                    f"/batches/MIX{i:02d}/retire", timeout=20
                ).status_code == 200
            else:
                # ...the others re-download what they just uploaded,
                # possibly from the other instance.
                other = base_urls[(i + 1) % 2]
                r = httpx.get(
                    f"{other}/batches/MIX{i:02d}/archive", timeout=20
                )
                assert r.status_code == 200
                assert r.content == body
            c.post(f"{base_urls[i % 2]}/gc/run", timeout=20)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(worker, range(n)))

    summary = httpx.get(
        f"{base_urls[0]}/storage/summary", timeout=10
    ).json()
    # Table and filesystem must agree exactly.
    assert summary["blob_rows"] == summary["blob_files_on_disk"]
    assert summary["queued_for_gc"] == 0
    assert summary["quarantined_files"] == 0

    # Every still-active batch must have a downloadable original.
    listing = httpx.get(
        f"{base_urls[1]}/batches", params={"status": "active"}, timeout=10
    ).json()
    assert listing["total"] == n // 2
    for batch in listing["batches"]:
        r = httpx.get(
            f"{base_urls[0]}/batches/{batch['batch_id']}/archive", timeout=10
        )
        assert r.status_code == 200
        assert hashlib.sha256(r.content).hexdigest() == batch["sha256"]


def test_concurrent_same_content_upload_then_cross_instance_download(
    two_instances, data_dir
):
    """Same SHA-256 arriving at both instances for two new batches."""
    base_urls = two_instances
    body = make_zip({"dup": b"identical-bytes"})
    digest = hashlib.sha256(body).hexdigest()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(
            lambda args: _upload(args[0], args[1], body),
            [(base_urls[0], "DUP-A"), (base_urls[1], "DUP-B")],
        ))
    assert results == [200, 200]

    assert len(list((data_dir / "blobs").iterdir())) == 1
    for url in base_urls:
        for batch in ("DUP-A", "DUP-B"):
            r = httpx.get(f"{url}/batches/{batch}/archive", timeout=10)
            assert r.status_code == 200
            assert hashlib.sha256(r.content).hexdigest() == digest
