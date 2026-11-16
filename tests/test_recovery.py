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


def test_missing_file_row_is_removed_not_reinserted(data_dir):
    """A ready row whose bytes are gone must be deleted, not resurrected.

    Regression: phase two used to re-insert a ``ready`` row for the same
    hash whenever it saw its own ``ready`` row, so an orphan row with no
    bytes survived every GC pass forever (``blob_rows`` never converged
    to ``blob_files_on_disk``).
    """
    from app import storage

    digest = hashlib.sha256(b"orphan-row").hexdigest()

    with TestClient(app) as c:
        pass                     # schema + empty-store reconcile

        # A ready row with no batch reference and no bytes anywhere.
        conn = sqlite3.connect(data_dir / "archives.db")
        conn.execute(
            "INSERT INTO blobs (sha256, size_bytes, status) "
            "VALUES (?, 999, 'ready')",
            (digest,),
        )
        conn.commit()
        conn.close()

        # Repeated GC runs must converge and stay converged.
        for _ in range(3):
            storage.run_gc()
            summary = c.get("/storage/summary").json()
            assert summary["blob_rows"] == 0
            assert summary["blob_files_on_disk"] == 0
            assert summary["queued_for_gc"] == 0


def test_mark_holds_write_lock_across_orphan_reaper(data_dir, monkeypatch):
    """The ledger read and orphan reaping must share one write lock.

    Regression: ``_mark`` read the blob ledger, then listed ``blobs/``
    with no lock in between.  An upload committing and publishing a new
    original in that window made the fresh file look rowless, so the
    reaper deleted an active batch's bytes.

    Observable invariant: while the reaper runs, no other process can
    take the write lock, so no upload can commit a row after the ledger
    was read.  (The upload publishes only after committing.)
    """
    import threading

    from app import config, storage

    with TestClient(app):
        pass                     # schema + empty-store reconcile

    observed: dict[str, bool] = {}
    real_iterdir = type(config.BLOB_DIR).iterdir

    def iterdir_with_concurrent_writer(self):
        def try_write() -> None:
            conn = sqlite3.connect(data_dir / "archives.db", timeout=0.2)
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("COMMIT")
                observed["acquired"] = True
            except sqlite3.OperationalError:
                observed["acquired"] = False
            finally:
                conn.close()

        thread = threading.Thread(target=try_write)
        thread.start()
        thread.join(timeout=5)
        return real_iterdir(self)

    monkeypatch.setattr(
        type(config.BLOB_DIR), "iterdir", iterdir_with_concurrent_writer
    )
    storage._mark(storage.GcReport())

    assert observed.get("acquired") is False


def test_sweep_does_not_strand_active_batch_when_upload_claims_blob(
    data_dir, monkeypatch
):
    """Upload/GC interleaving must never leave an active batch without bytes.

    Deterministic version of the failure the race hammer found: the sweep
    marks a blob ``deleting``, an upload claims the same hash for a new
    batch (flipping it back to ``ready`` + publishing), and the sweep must
    not then move that original into quarantine and delete it.
    """
    from app import db, storage

    body = make_zip({"interleave": b"upload-claims-while-sweeping"})
    digest = hashlib.sha256(body).hexdigest()

    with TestClient(app) as c:
        assert upload(c, "DOOMED", body).status_code == 200
        # Retire via SQL so GC has not run yet (the queue is filled below).
        conn = sqlite3.connect(data_dir / "archives.db")
        conn.execute(
            "UPDATE batches SET status='retired' WHERE batch_id='DOOMED'"
        )
        conn.commit()
        conn.close()

        # Mark and produce the exact post-phase-one state without removing
        # the bytes yet, so the upload can claim the hash in between.
        storage._mark(storage.GcReport())
        with db.transaction() as conn:
            conn.execute(
                "UPDATE blobs SET status='deleting', quarantine_path=? "
                "WHERE sha256=?",
                (str(data_dir / "quarantine" / digest), digest),
            )

        # A new active batch claims the same content (what upload_archive
        # does in its transaction: deleting -> ready, publish bytes).
        with db.transaction() as conn:
            conn.execute(
                "UPDATE blobs SET status='ready', quarantine_path=NULL "
                "WHERE sha256=?", (digest,),
            )
            conn.execute("DELETE FROM gc_queue WHERE sha256=?", (digest,))
            conn.execute(
                "INSERT INTO batches (batch_id, sha256, size_bytes, status) "
                "VALUES ('NEW', ?, ?, 'active')",
                (digest, len(body)),
            )

        # A full GC pass now must keep the bytes downloadable.
        storage.run_gc()
        assert (data_dir / "blobs" / digest).exists()
        assert not (data_dir / "quarantine" / digest).exists()
        got = c.get("/batches/NEW/archive")
        assert got.status_code == 200
        assert hashlib.sha256(got.content).hexdigest() == digest
        summary = c.get("/storage/summary").json()
        assert summary["blob_rows"] == summary["blob_files_on_disk"] == 1
        assert summary["queued_for_gc"] == 0


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
