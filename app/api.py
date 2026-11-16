"""FastAPI application: batch archive intake, download, retirement, GC.

Concurrency model
-----------------
All mutating endpoints run their checks and writes inside one
``BEGIN IMMEDIATE`` transaction, so two instances sharing the volume
serialize on SQLite's write lock and cannot observe each other's
half-committed state.  Downloads open the content file and stream it
outside any database transaction; on POSIX an unlinked (or quarantined)
file stays readable through the already-open file descriptor, so a slow
download always finishes and never blocks another batch's retirement.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel

from . import config, db, storage


# --------------------------------------------------------------------------- #
# Response models
# --------------------------------------------------------------------------- #

class BatchSummary(BaseModel):
    batch_id: str
    sha256: str
    size_bytes: int
    status: str
    created_at: str
    retired_at: str | None


class BatchListResponse(BaseModel):
    batches: list[BatchSummary]
    total: int


class StorageSummary(BaseModel):
    active_batches: int
    retired_batches: int
    # Rows in the blobs table - the number of originals that must exist.
    blob_rows: int
    # Physical files actually present in blobs/ - compare with blob_rows
    # to verify garbage collection finished its work.
    blob_files_on_disk: int
    quarantined_files: int
    queued_for_gc: int
    bytes_used_by_blobs: int


class GcResponse(BaseModel):
    marked: list[str]
    deleted_blobs: list[str]
    restored_blobs: list[str]
    reaped_unknown_files: list[str]
    swept_queued: int


class UploadResponse(BaseModel):
    batch_id: str
    sha256: str
    size_bytes: int
    reused_existing_content: bool


# --------------------------------------------------------------------------- #
# Lifespan
# --------------------------------------------------------------------------- #

@asynccontextmanager
async def lifespan(app: FastAPI) -> Any:
    db.init_db()
    # Resume any GC work interrupted by a previous process exit.
    storage.reconcile()
    yield


app = FastAPI(
    title="Sequencing Center Archive Reclamation API",
    version="1.0.0",
    description=(
        "Upload per-batch ZIP archives (deduplicated by SHA-256), "
        "download original bytes, retire batches and garbage-collect "
        "originals that no batch references anymore."
    ),
    lifespan=lifespan,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _batch_to_model(row: Any) -> BatchSummary:
    return BatchSummary(
        batch_id=row["batch_id"],
        sha256=row["sha256"],
        size_bytes=row["size_bytes"],
        status=row["status"],
        created_at=row["created_at"],
        retired_at=row["retired_at"],
    )


def _validate_batch_id(batch_id: str) -> None:
    """Keep batch identifiers to a sane, path-safe charset."""
    if not batch_id or len(batch_id) > 128:
        raise HTTPException(
            status_code=400,
            detail="batch_id must be 1..128 characters",
        )
    if any(ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._"
           for ch in batch_id):
        raise HTTPException(
            status_code=400,
            detail="batch_id may only contain letters, digits, '-', '.', '_'",
        )


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #

@app.post("/batches/{batch_id}/archive", response_model=UploadResponse)
def upload_archive(batch_id: str, file: UploadFile = File(...)) -> UploadResponse:
    """Store a batch's ZIP archive (max 2 MiB).

    * Same batch + same SHA-256 re-upload is a no-op success (reuse).
    * Same batch + different SHA-256 is rejected with 409.
    * Retired batch numbers can never be reused (409).
    * Identical SHA-256 content is stored exactly once on disk.
    """
    _validate_batch_id(batch_id)
    try:
        body, sha256 = storage.read_and_validate(file.file, config.MAX_UPLOAD_BYTES)
    except storage.UploadError as exc:
        raise HTTPException(status_code=413 if "exceeds" in str(exc) else 400,
                            detail=str(exc)) from exc

    # Stage bytes before taking the write lock so the critical section
    # only contains database work and a fast rename.
    tmp = storage.stage_tmp(body)

    try:
        reused = False
        with db.transaction() as conn:
            batch = conn.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()

            if batch is not None:
                if batch["status"] == "retired":
                    raise HTTPException(
                        status_code=409,
                        detail=f"batch '{batch_id}' was retired and its number "
                               "can never be reused",
                    )
                if batch["sha256"] != sha256:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"batch '{batch_id}' already holds content "
                            f"{batch['sha256']}; refusing to overwrite with "
                            f"different content {sha256}"
                        ),
                    )
                reused = True

            blob = conn.execute(
                "SELECT * FROM blobs WHERE sha256 = ?", (sha256,)
            ).fetchone()
            if blob is None:
                conn.execute(
                    "INSERT INTO blobs (sha256, size_bytes, status) "
                    "VALUES (?, ?, 'ready')",
                    (sha256, len(body)),
                )
            elif blob["status"] == "deleting":
                # GC had marked this content for removal, but a batch now
                # references it: reclaim it.
                conn.execute(
                    "UPDATE blobs SET status = 'ready', quarantine_path = NULL "
                    "WHERE sha256 = ?",
                    (sha256,),
                )
                conn.execute(
                    "DELETE FROM gc_queue WHERE sha256 = ?", (sha256,)
                )

            if batch is None:
                conn.execute(
                    "INSERT INTO batches (batch_id, sha256, size_bytes, status) "
                    "VALUES (?, ?, ?, 'active')",
                    (batch_id, sha256, len(body)),
                )

        # Transaction committed: publish the bytes (idempotent).  If GC
        # moved them to quarantine in parallel, move them back; if another
        # instance already published identical bytes, nothing to do.
        storage.publish(tmp, sha256)
        if storage.quarantine_path(sha256).exists():
            storage.move_back_from_quarantine(sha256)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise

    return UploadResponse(
        batch_id=batch_id,
        sha256=sha256,
        size_bytes=len(body),
        reused_existing_content=reused,
    )


@app.get("/batches/{batch_id}/archive")
def download_archive(batch_id: str) -> FileResponse:
    """Download the original uploaded bytes for an active batch.

    Returns 404 for unknown or retired batches (retired numbers stay
    dead).  Once the response starts, it streams from an open file
    descriptor, so concurrent garbage collection cannot interrupt it.
    """
    _validate_batch_id(batch_id)
    conn = db.get_conn()
    batch = conn.execute(
        "SELECT * FROM batches WHERE batch_id = ? AND status = 'active'",
        (batch_id,),
    ).fetchone()
    if batch is None:
        raise HTTPException(
            status_code=404,
            detail=f"no active batch '{batch_id}'",
        )

    path = Path(config.BLOB_DIR) / batch["sha256"]
    if not path.exists():
        # Transient crash-recovery window: the row is live but bytes are
        # being restored.  Report missing rather than stream a broken file.
        raise HTTPException(
            status_code=404,
            detail=f"original for batch '{batch_id}' is currently unavailable",
        )

    # FileResponse opens the descriptor now and keeps it for the whole
    # (possibly slow) transfer.  No DB transaction is held meanwhile.
    return FileResponse(
        path,
        media_type="application/zip",
        filename=f"{batch_id}.zip",
    )


@app.post("/batches/{batch_id}/retire", status_code=200)
def retire_batch(batch_id: str) -> dict[str, Any]:
    """Retire a batch. Its number becomes permanently unusable.

    Retirement only flips batch state; the attached original is reclaimed
    by GC once no *active* batch references it.  GC runs inline after the
    state change so the space is released promptly.  The endpoint is
    idempotent: retiring an already-retired batch returns 200.
    """
    _validate_batch_id(batch_id)
    with db.transaction() as conn:
        batch = conn.execute(
            "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        if batch is None:
            raise HTTPException(
                status_code=404, detail=f"unknown batch '{batch_id}'"
            )
        already = batch["status"] == "retired"
        if not already:
            conn.execute(
                "UPDATE batches SET status = 'retired', "
                "retired_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') "
                "WHERE batch_id = ?",
                (batch_id,),
            )

    # Outside the transaction: GC takes its own short locks per blob,
    # keeping a slow download (which holds no DB lock) completely out of
    # the way of other batches' retirement.
    report = storage.run_gc()
    return {
        "batch_id": batch_id,
        "status": "retired",
        "already_retired": already,
        "gc": report.__dict__,
    }


@app.post("/gc/run", response_model=GcResponse)
def trigger_gc() -> GcResponse:
    """Manually run the mark-and-sweep garbage collection."""
    report = storage.run_gc()
    return GcResponse(**report.__dict__)


@app.get("/batches", response_model=BatchListResponse)
def list_batches(status: str | None = None) -> BatchListResponse:
    """List batch summaries, optionally filtered by status."""
    conn = db.get_conn()
    if status is not None:
        if status not in ("active", "retired"):
            raise HTTPException(
                status_code=400,
                detail="status filter must be 'active' or 'retired'",
            )
        rows = conn.execute(
            "SELECT * FROM batches WHERE status = ? ORDER BY batch_id",
            (status,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM batches ORDER BY batch_id"
        ).fetchall()
    return BatchListResponse(
        batches=[_batch_to_model(row) for row in rows], total=len(rows)
    )


@app.get("/batches/{batch_id}", response_model=BatchSummary)
def get_batch(batch_id: str) -> BatchSummary:
    """Return one batch's summary."""
    _validate_batch_id(batch_id)
    row = db.get_conn().execute(
        "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404, detail=f"unknown batch '{batch_id}'"
        )
    return _batch_to_model(row)


@app.get("/storage/summary", response_model=StorageSummary)
def storage_summary() -> StorageSummary:
    """Inventory view for reconciling reclamation results."""
    conn = db.get_conn()
    active = conn.execute(
        "SELECT COUNT(*) FROM batches WHERE status = 'active'"
    ).fetchone()[0]
    retired = conn.execute(
        "SELECT COUNT(*) FROM batches WHERE status = 'retired'"
    ).fetchone()[0]
    blob_rows = conn.execute("SELECT COUNT(*) FROM blobs").fetchone()[0]
    queued = conn.execute("SELECT COUNT(*) FROM gc_queue").fetchone()[0]

    blob_files = [p for p in config.BLOB_DIR.iterdir() if p.is_file()]
    quarantined = [p for p in config.QUARANTINE_DIR.iterdir() if p.is_file()]
    return StorageSummary(
        active_batches=active,
        retired_batches=retired,
        blob_rows=blob_rows,
        blob_files_on_disk=len(blob_files),
        quarantined_files=len(quarantined),
        queued_for_gc=queued,
        bytes_used_by_blobs=sum(p.stat().st_size for p in blob_files),
    )


@app.get("/healthz")
async def healthz() -> Response:
    db.get_conn().execute("SELECT 1")
    return Response(content="ok", media_type="text/plain")


@app.post("/internal/shutdown", include_in_schema=False)
def shutdown() -> Response:
    """Cooperative process shutdown for tests and orchestration.

    Disabled unless ``ENABLE_SHUTDOWN_ENDPOINT=1``: production containers
    are stopped with SIGTERM, and this endpoint must not be reachable by
    callers of the archive API.  When enabled it schedules a hard but
    orderly exit from a background thread (the response is flushed first)
    for runtimes whose seccomp policy forbids signaling child processes.
    """
    import os

    if os.environ.get("ENABLE_SHUTDOWN_ENDPOINT") != "1":
        raise HTTPException(status_code=404, detail="not found")

    import threading

    def _stop() -> None:
        import time

        time.sleep(0.2)  # let the HTTP response leave the socket
        os._exit(0)

    threading.Thread(target=_stop, daemon=True).start()
    return Response(content="shutting down", media_type="text/plain")
