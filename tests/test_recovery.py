"""Crash recovery tests: GC interrupted by a hard process exit.

Scenarios
---------
1. Crash in phase 1 (row flipped to 'deleting', bytes moved away) and
   in phase 2 (bytes in quarantine, row still present): on restart,
   reconciliation must finish removing the now-unreferenced original.
2. After the same crash, the content becomes referenced again by a new
   batch: reconciliation must restore the bytes instead of deleting them.
3. The upload API itself reclaims a blob stuck in the 'deleting' state.
"""
from __future__ import annotations

import hashlib
import sqlite3
import subprocess
import sys
from pathlib import Path

from fastapi.testclient import TestClient

from app.api import app
from conftest import make_zip, upload

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "scripts" / "crash_worker.py"


def _run_worker(data_dir: Path, hook: str) -> subprocess.CompletedProcess:
    # -I keeps the child interpreter isolated from PYTHONPATH pollution.
    return subprocess.run(
        [sys.executable, "-I", str(WORKER), str(data_dir), hook],
        capture_output=True,
        text=True,
        timeout=60,
    )


def _stage_retired_without_gc(data_dir: Path, batch_id: str, body: bytes) -> str:
    """Create an active batch via the API, then retire it via raw SQL.

    This reproduces the exact pre-GC state (retired batch, live blob,
    gc_queue empty) that a crash worker can then interrupt.
    """
    digest = hashlib.sha256(body).hexdigest()
    with TestClient(app) as c:
        assert upload(c, batch_id, body).status_code == 200

    conn = sqlite3.connect(data_dir / "archives.db")
    conn.execute(
        "UPDATE batches SET status = 'retired', "
        "retired_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') "
        "WHERE batch_id = ?",
        (batch_id,),
    )
    conn.commit()
    conn.close()
    assert (data_dir / "blobs" / digest).exists()
    return digest


def test_recovery_finishes_reclamation_after_phase1_crash(data_dir):
    body = make_zip({"doomed": b"ephemeral"})
    digest = _stage_retired_without_gc(data_dir, "DOOMED", body)

    proc = _run_worker(data_dir, "phase1")
    assert proc.returncode == 2, proc.stderr  # hard exit code
    # Interrupted between the row flip and the file move: the blob row is
    # stuck in 'deleting' while its bytes still sit in blobs/.
    row = sqlite3.connect(data_dir / "archives.db").execute(
        "SELECT status FROM blobs WHERE sha256 = ?", (digest,)
    ).fetchone()
    assert row == ("deleting",)
    assert (data_dir / "blobs" / digest).exists()

    with TestClient(app) as c:  # lifespan -> reconcile() resumes the sweep
        assert not (data_dir / "blobs" / digest).exists()
        assert not (data_dir / "quarantine" / digest).exists()
        summary = c.get("/storage/summary").json()
        assert summary["blob_rows"] == 0
        assert summary["blob_files_on_disk"] == 0
        assert summary["queued_for_gc"] == 0
        assert summary["quarantined_files"] == 0


def test_recovery_finishes_reclamation_after_phase2_crash(data_dir):
    body = make_zip({"doomed2": b"ephemeral-too"})
    digest = _stage_retired_without_gc(data_dir, "DOOMED", body)

    proc = _run_worker(data_dir, "phase2")
    assert proc.returncode == 2, proc.stderr
    assert (data_dir / "quarantine" / digest).exists()

    with TestClient(app) as c:
        summary = c.get("/storage/summary").json()
        assert summary["blob_rows"] == summary["blob_files_on_disk"] == 0
        assert summary["quarantined_files"] == 0
        assert summary["queued_for_gc"] == 0


def test_referenced_again_after_crash_is_restored(data_dir):
    body = make_zip({"rescued": b"keep-me"})
    digest = _stage_retired_without_gc(data_dir, "OLD", body)

    proc = _run_worker(data_dir, "phase1")
    assert proc.returncode == 2, proc.stderr
    # Row stuck in 'deleting' before the bytes were moved.
    row = sqlite3.connect(data_dir / "archives.db").execute(
        "SELECT status FROM blobs WHERE sha256 = ?", (digest,)
    ).fetchone()
    assert row == ("deleting",)

    # While the API was down, a new batch starts referencing the same
    # content hash (a concurrent instance committing the same upload).
    conn = sqlite3.connect(data_dir / "archives.db")
    conn.execute(
        "INSERT INTO batches (batch_id, sha256, size_bytes, status) "
        "VALUES (?, ?, ?, 'active')",
        ("NEW", digest, len(body)),
    )
    conn.commit()
    conn.close()

    with TestClient(app) as c:  # reconcile must restore, not delete
        assert (data_dir / "blobs" / digest).exists()
        assert not (data_dir / "quarantine" / digest).exists()

        row = sqlite3.connect(data_dir / "archives.db").execute(
            "SELECT status FROM blobs WHERE sha256 = ?", (digest,)
        ).fetchone()
        assert row == ("ready",)

        got = c.get("/batches/NEW/archive")
        assert got.status_code == 200
        assert hashlib.sha256(got.content).hexdigest() == digest
        summary = c.get("/storage/summary").json()
        assert summary["blob_rows"] == summary["blob_files_on_disk"] == 1
        assert summary["queued_for_gc"] == 0


def test_upload_reclaims_blob_stuck_in_deleting(data_dir):
    body = make_zip({"mid-gc": b"rescued-by-upload"})
    digest = hashlib.sha256(body).hexdigest()

    with TestClient(app) as c:
        upload(c, "OLD", body)

        # Reproduce the exact post-phase-1 state without hard-exiting.
        from app import db, storage
        with db.transaction() as conn:
            conn.execute(
                "UPDATE blobs SET status = 'deleting', "
                "quarantine_path = ? WHERE sha256 = ?",
                (str(storage.quarantine_path(digest)), digest),
            )
        storage.config.QUARANTINE_DIR.mkdir(exist_ok=True)
        storage.os.replace(
            storage.blob_path(digest), storage.quarantine_path(digest)
        )
        assert not (data_dir / "blobs" / digest).exists()

        # A new batch uploads identical content: the API must claim the
        # deleting row and move the bytes back.
        r = upload(c, "NEW", body)
        assert r.status_code == 200, r.text
        assert (data_dir / "blobs" / digest).exists()
        assert not (data_dir / "quarantine" / digest).exists()
        assert c.get("/batches/NEW/archive").content == body


def test_orphan_tmp_parts_are_reaped_on_restart(data_dir):
    body = make_zip({"z": b"abc"})
    with TestClient(app) as c:
        upload(c, "T1", body)

    # Simulate an upload that hard-crashed after staging its temp part.
    (data_dir / "tmp").mkdir(exist_ok=True)
    orphan = data_dir / "tmp" / "part-deadbeef"
    orphan.write_bytes(b"stuck")

    with TestClient(app) as c:
        assert c.get("/healthz").status_code == 200
        assert not orphan.exists()
        # Existing data untouched.
        assert c.get("/batches/T1/archive").status_code == 200


def test_staged_part_rescues_missing_blob_after_crash(data_dir):
    """Crash between DB COMMIT and file publish must not strand a batch."""
    body = make_zip({"late": b"published-after-restart"})
    digest = hashlib.sha256(body).hexdigest()

    # Build the exact post-commit/pre-publish state via SQL + a tmp part.
    from app import config
    with TestClient(app):  # creates schema, runs reconcile on empty store
        pass
    conn = sqlite3.connect(data_dir / "archives.db")
    conn.execute(
        "INSERT INTO blobs (sha256, size_bytes, status) VALUES (?, ?, 'ready')",
        (digest, len(body)),
    )
    conn.execute(
        "INSERT INTO batches (batch_id, sha256, size_bytes, status) "
        "VALUES (?, ?, ?, 'active')",
        ("LATE", digest, len(body)),
    )
    conn.commit()
    conn.close()
    part = config.TMP_DIR / "part-crashed"
    part.write_bytes(body)
    # The blob row exists but its bytes do not.
    assert not (data_dir / "blobs" / digest).exists()

    with TestClient(app) as c:
        got = c.get("/batches/LATE/archive")
        assert got.status_code == 200
        assert hashlib.sha256(got.content).hexdigest() == digest
        assert (data_dir / "blobs" / digest).exists()
        assert not part.exists()
        summary = c.get("/storage/summary").json()
        assert summary["blob_rows"] == summary["blob_files_on_disk"] == 1
