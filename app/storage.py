"""Content-addressed blob storage and crash-safe garbage collection.

Layout on the shared volume::

    blobs/<sha256>            one physical copy per distinct archive
    quarantine/<sha256>       blob moved out of blobs/ while being removed
    tmp/part-<uuid>           in-flight upload bodies

Garbage collection is split into two phases so it is safe across two
instances and across crashes:

* **mark** - every blob with no active batch reference is recorded in
  ``gc_queue``; files on disk that no blob row knows about are reaped.
  The ledger read, the directory listing and the unlinks all happen under
  the write lock, so a file published by a racing upload can never be
  mistaken for junk.
* **sweep** - each queued blob is re-checked.  If it has gained a
  reference again (a new upload reused the hash) it is kept/restored.
  Otherwise the row is flipped to ``deleting`` (guarded on ``ready`` and
  on the absence of a live reference), the bytes move to quarantine, and
  a final write-locked transaction re-checks references, deletes the row
  and unlinks both copies atomically with respect to uploads.  A crash
  between steps leaves a ``deleting`` row or a quarantine file that
  :func:`reconcile` resumes on restart.

Downloads (:func:`open_original`) pin the inode before any response
header is committed, so reclamation on either instance cannot truncate an
in-flight transfer.

Upload ordering (also crash-safe):

1. stream + validate + hash into a ``tmp/`` part,
2. BEGIN IMMEDIATE, validate batch rules, insert/claim the blob row,
3. COMMIT, then atomically rename the part into ``blobs/`` if missing.

The row always exists before the file appears, so the mark reaper can
never mistake a fresh upload for an orphan, and every upload guarantees
the physical bytes for the row it claims.  GC's delete decisions are
taken while holding the same write lock, so they can never race that
ordering.
"""
from __future__ import annotations

import hashlib
import io
import os
import stat
import uuid
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO

from . import config, db

# Test hook: when set, the GC process exits (os._exit) at this checkpoint.
# Used by the crash-recovery tests; never set in production.
CRASH_HOOK: str | None = None


class UploadError(ValueError):
    """Raised for client-side upload problems (size, ZIP validity)."""


def _fsync_dir(path: Path) -> None:
    """Durably persist a rename/unlink inside a directory."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def blob_path(sha256: str) -> Path:
    return config.BLOB_DIR / sha256


def quarantine_path(sha256: str) -> Path:
    return config.QUARANTINE_DIR / sha256


# --------------------------------------------------------------------------- #
# Upload intake
# --------------------------------------------------------------------------- #

def read_and_validate(stream: BinaryIO, max_bytes: int) -> tuple[bytes, str]:
    """Read an upload body, enforcing the size cap and ZIP validity.

    The body is buffered in memory: 2 MiB is small enough and keeps the
    hashing / zip checks simple before anything touches the shared volume.
    Archive entries are never extracted, so a malicious ZIP cannot write
    outside the volume.
    """
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = stream.read(64 * 1024)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise UploadError(f"upload exceeds {max_bytes} bytes")
        chunks.append(chunk)
    body = b"".join(chunks)
    if not body:
        raise UploadError("upload body is empty")
    if not zipfile.is_zipfile(io.BytesIO(body)):
        raise UploadError("uploaded content is not a valid ZIP archive")
    return body, hashlib.sha256(body).hexdigest()


def stage_tmp(body: bytes) -> Path:
    """Durably write the body to a temp part (never scanned by the reaper)."""
    config.TMP_DIR.mkdir(parents=True, exist_ok=True)
    tmp = config.TMP_DIR / f"part-{uuid.uuid4().hex}"
    with open(tmp, "wb") as fh:
        fh.write(body)
        fh.flush()
        os.fsync(fh.fileno())
    return tmp


def publish(tmp: Path, sha256: str) -> None:
    """Move a staged part into blobs/ if the bytes are not already there."""
    config.BLOB_DIR.mkdir(parents=True, exist_ok=True)
    target = blob_path(sha256)
    if not target.exists():
        os.replace(tmp, target)
        _fsync_dir(config.BLOB_DIR)
    else:
        tmp.unlink(missing_ok=True)


# --------------------------------------------------------------------------- #
# Pinned reads (download safety)
# --------------------------------------------------------------------------- #

class OriginalUnavailable(Exception):
    """The original is not readable right now (retired, GC'd, restoring)."""


def open_original(sha256: str) -> tuple[int, os.stat_result]:
    """Open the original's bytes and pin the inode with an OS descriptor.

    The returned descriptor is opened *before* any HTTP response header is
    committed.  Once open, a concurrent rename into ``quarantine/`` or an
    unlink by either instance cannot affect the readable inode (POSIX), so
    the download can always deliver exactly the bytes its headers promise.

    If the directory entry in ``blobs/`` is momentarily gone because GC
    moved the bytes to ``quarantine/`` while a live reference was being
    re-established, the quarantined copy (same content-addressed inode) is
    used instead.  Once the descriptor is open, both paths may vanish.

    The file must be a regular file.  Anything else is reported as
    :class:`OriginalUnavailable` rather than streaming something the
    client did not ask for.  The caller owns the descriptor and must
    close it.
    """
    last_error: OSError | None = None
    for path in (blob_path(sha256), quarantine_path(sha256)):
        try:
            fd = os.open(path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
        except FileNotFoundError as exc:
            last_error = exc
            continue
        except NotADirectoryError as exc:
            last_error = exc
            continue
        try:
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode):
                raise OriginalUnavailable(sha256)
            return fd, st
        except BaseException:
            os.close(fd)
            raise
    raise OriginalUnavailable(sha256) from last_error


# --------------------------------------------------------------------------- #
# Garbage collection
# --------------------------------------------------------------------------- #

@dataclass
class GcReport:
    reaped_unknown_files: list[str] = field(default_factory=list)
    marked: list[str] = field(default_factory=list)
    deleted_blobs: list[str] = field(default_factory=list)
    restored_blobs: list[str] = field(default_factory=list)
    swept_queued: int = 0


def move_back_from_quarantine(sha256: str) -> None:
    """Return quarantined bytes to blobs/ (best effort, idempotent)."""
    qpath = quarantine_path(sha256)
    bpath = blob_path(sha256)
    if qpath.exists():
        if bpath.exists():
            # An upload already put fresh identical bytes back.
            qpath.unlink(missing_ok=True)
        else:
            os.replace(qpath, bpath)
            _fsync_dir(config.BLOB_DIR)


def _mark(report: GcReport) -> None:
    conn = db.get_conn()

    # Reap physical files that no blob row accounts for.  Run the re-check
    # and the unlink under the write lock: uploads commit their row before
    # publishing bytes, so with the lock held a file that some row names
    # can never be mistaken for junk (and an upload that has not committed
    # yet cannot rename a file into blobs/ until we release).  Without
    # this, a read of the ledger followed by a directory listing could
    # delete a freshly published original of an active batch.
    with db.transaction():
        known = {
            row["sha256"] for row in conn.execute("SELECT sha256 FROM blobs")
        }
        on_disk = {p.name for p in config.BLOB_DIR.iterdir() if p.is_file()}
        for name in sorted(on_disk - known):
            try:
                (config.BLOB_DIR / name).unlink()
            except FileNotFoundError:
                pass
            report.reaped_unknown_files.append(name)

    # Queue every ready blob without an active batch reference.
    rows = conn.execute(
        """
        SELECT b.sha256
          FROM blobs b
         WHERE b.status = 'ready'
           AND NOT EXISTS (
                 SELECT 1 FROM batches t
                  WHERE t.sha256 = b.sha256 AND t.status = 'active'
               )
        """
    ).fetchall()
    if rows:
        with db.transaction():
            conn.executemany(
                "INSERT OR IGNORE INTO gc_queue (sha256) VALUES (?)",
                [(row["sha256"],) for row in rows],
            )
        report.marked = [row["sha256"] for row in rows]


def _finish_phase_two(sha256: str, report: GcReport) -> None:
    """Delete the blob row under the write lock, then remove its bytes.

    References are re-checked inside the transaction: a concurrently
    committed upload reusing this hash forces a restore instead of
    deletion.  If an upload re-created a *new* ``ready`` row while the
    old one sat in ``deleting``, the bytes are kept in quarantine for
    that upload to republish rather than unlinked.
    """
    conn = db.get_conn()
    with db.transaction():
        referenced = conn.execute(
            "SELECT 1 FROM batches WHERE sha256 = ? AND status = 'active' "
            "LIMIT 1",
            (sha256,),
        ).fetchone()
        if referenced:
            conn.execute(
                "UPDATE blobs SET status = 'ready', quarantine_path = NULL "
                "WHERE sha256 = ?",
                (sha256,),
            )
            conn.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha256,))
            restored = True
        else:
            # No live reference: drop the row.  Do NOT re-insert it.  A
            # racing upload cannot hold a row here (we hold the write
            # lock); it can only commit *after* this transaction, and its
            # own publish step materializes the bytes.  Re-inserting a
            # row for a missing file used to strand a "ready" row with no
            # bytes that every later sweep re-inserted forever.
            conn.execute("DELETE FROM blobs WHERE sha256 = ?", (sha256,))
            conn.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha256,))
            restored = False

    if restored:
        move_back_from_quarantine(sha256)
        report.restored_blobs.append(sha256)
        return

    # Decide and unlink under the write lock.  An upload commits its blob
    # row *before* it renames bytes into blobs/, so while we hold the lock
    # no new row can appear, and any publish that happens now belongs to a
    # row we can see.  That makes "is there a live row?" + "delete the
    # leftovers" atomic against uploads: an original that a live row names
    # is never unlinked.
    qpath = quarantine_path(sha256)
    with db.transaction():
        live = conn.execute(
            "SELECT 1 FROM blobs WHERE sha256 = ?", (sha256,)
        ).fetchone()
        if live is not None:
            # A same-hash upload committed a replacement row (and may have
            # published its own bytes).  Keep exactly one copy and let the
            # upload's publish/move_back step own the final layout.
            if blob_path(sha256).exists():
                qpath.unlink(missing_ok=True)
            else:
                move_back_from_quarantine(sha256)
            report.restored_blobs.append(sha256)
            return

        # No row references these bytes: reclaim both copies.
        qpath.unlink(missing_ok=True)
        blob_path(sha256).unlink(missing_ok=True)

    _fsync_dir(config.QUARANTINE_DIR)
    report.deleted_blobs.append(sha256)


def _sweep_one(sha256: str, report: GcReport) -> None:
    conn = db.get_conn()
    row = conn.execute(
        "SELECT status, quarantine_path FROM blobs WHERE sha256 = ?",
        (sha256,),
    ).fetchone()

    if row is None:
        # Another instance already finished phase two.
        with db.transaction():
            conn.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha256,))
        report.swept_queued += 1
        return

    if row["status"] == "deleting":
        # Resume after a crash between the move and the row deletion.
        if CRASH_HOOK == "phase2":
            os._exit(2)
        _finish_phase_two(sha256, report)
        report.swept_queued += 1
        return

    # status == 'ready': re-check references before touching the bytes.
    referenced = conn.execute(
        "SELECT 1 FROM batches WHERE sha256 = ? AND status = 'active' LIMIT 1",
        (sha256,),
    ).fetchone()
    if referenced:
        with db.transaction():
            conn.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha256,))
        report.restored_blobs.append(sha256)
        return

    bpath = blob_path(sha256)
    if not bpath.exists():
        # Bytes already gone (crash/reaper); finish the bookkeeping.
        _finish_phase_two(sha256, report)
        report.swept_queued += 1
        return

    # Phase one: flip the row to 'deleting' under the write lock, but only
    # if the row really is still ''ready'' and still unreferenced.  If a
    # same-hash upload claimed it in the meantime (re-upload sets it back
    # to ''ready'' after a sweep marked it), the guarded UPDATE changes no
    # rows; re-evaluate instead of moving bytes a live batch still needs.
    with db.transaction():
        still_unreferenced = conn.execute(
            "SELECT 1 FROM batches WHERE sha256 = ? AND status = 'active' "
            "LIMIT 1",
            (sha256,),
        ).fetchone() is None
        if not still_unreferenced:
            conn.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha256,))
            report.restored_blobs.append(sha256)
            return
        updated = conn.execute(
            "UPDATE blobs SET status = 'deleting', quarantine_path = ? "
            "WHERE sha256 = ? AND status = 'ready'",
            (str(quarantine_path(sha256)), sha256),
        ).rowcount
    if not updated:
        # Someone else (another instance, or an upload) changed the row.
        _sweep_one(sha256, report)
        return
    if CRASH_HOOK == "phase1":
        os._exit(2)
    qpath = quarantine_path(sha256)
    try:
        os.replace(bpath, qpath)
    except FileNotFoundError:
        # Another instance won the race and moved the bytes first.
        # Re-read the row and let the idempotent branches below decide
        # whether to finish the deletion or restore a re-referenced blob.
        row = conn.execute(
            "SELECT status FROM blobs WHERE sha256 = ?", (sha256,)
        ).fetchone()
        if row is None:
            with db.transaction():
                conn.execute(
                    "DELETE FROM gc_queue WHERE sha256 = ?", (sha256,)
                )
            report.swept_queued += 1
            return
        _sweep_one(sha256, report)
        return
    _fsync_dir(config.QUARANTINE_DIR)

    if CRASH_HOOK == "phase2":
        os._exit(2)
    _finish_phase_two(sha256, report)
    report.swept_queued += 1


def run_gc() -> GcReport:
    """Mark unreferenced originals and sweep everything queued."""
    report = GcReport()
    config.ensure_layout()
    _mark(report)

    conn = db.get_conn()
    queued = [r["sha256"] for r in conn.execute("SELECT sha256 FROM gc_queue")]
    for sha256 in queued:
        _sweep_one(sha256, report)
    return report


# --------------------------------------------------------------------------- #
# Startup reconciliation (crash recovery)
# --------------------------------------------------------------------------- #

def _restore_blob_row(sha256: str) -> None:
    """Mark a deleting blob live again and return its bytes."""
    conn = db.get_conn()
    with db.transaction():
        conn.execute(
            "UPDATE blobs SET status = 'ready', quarantine_path = NULL "
            "WHERE sha256 = ?",
            (sha256,),
        )
        conn.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha256,))
    move_back_from_quarantine(sha256)


def reconcile() -> GcReport:
    """Resume interrupted GC work after a process exit/restart."""
    config.ensure_layout()

    # 1. In-flight upload parts never became rows; drop them - unless a
    #    referenced blob is missing its bytes (crash between COMMIT and
    #    publish), in which case the part is the only surviving copy and
    #    must be moved into place so an active batch never lacks its file.
    tmp_parts = list(config.TMP_DIR.glob("part-*"))
    if tmp_parts:
        missing = {
            r["sha256"]
            for r in db.get_conn().execute(
                "SELECT b.sha256 FROM blobs b "
                "WHERE b.status = 'ready' "
                "AND EXISTS (SELECT 1 FROM batches t "
                "            WHERE t.sha256 = b.sha256 AND t.status = 'active')"
            )
        }
        missing = {h for h in missing if not blob_path(h).exists()}
        for part in tmp_parts:
            rescued = False
            if missing:
                digest = hashlib.sha256(part.read_bytes()).hexdigest()
                if digest in missing:
                    os.replace(part, blob_path(digest))
                    _fsync_dir(config.BLOB_DIR)
                    missing.discard(digest)
                    rescued = True
            if not rescued:
                part.unlink(missing_ok=True)

    conn = db.get_conn()

    # 2. Blobs stuck in 'deleting': requeue; restore any that are
    #    referenced again, otherwise let the sweep finish them.
    deleting = [
        r["sha256"]
        for r in conn.execute(
            "SELECT sha256 FROM blobs WHERE status = 'deleting'"
        )
    ]
    for sha256 in deleting:
        conn.execute(
            "INSERT OR IGNORE INTO gc_queue (sha256) VALUES (?)", (sha256,)
        )
        referenced = conn.execute(
            "SELECT 1 FROM batches WHERE sha256 = ? AND status = 'active' "
            "LIMIT 1",
            (sha256,),
        ).fetchone()
        if referenced:
            _restore_blob_row(sha256)

    # 3. Quarantine files without a deleting row (unlink missed by crash).
    tracked_q = {
        r["sha256"]
        for r in conn.execute(
            "SELECT sha256 FROM blobs WHERE quarantine_path IS NOT NULL"
        )
    }
    for qfile in config.QUARANTINE_DIR.iterdir():
        if qfile.is_file() and qfile.name not in tracked_q:
            qfile.unlink(missing_ok=True)

    # 4. Continue the normal mark/sweep pipeline.
    return run_gc()
