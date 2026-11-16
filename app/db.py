"""SQLite access shared by every worker thread / process.

Design notes
------------
* WAL + ``busy_timeout`` let two API instances on the same volume take
  short write turns without seeing "database is locked".
* Each thread keeps its own connection (check_same_thread is per
  connection) and every connection is opened in autocommit mode; the
  ``transaction()`` context manager supplies explicit BEGIN/COMMIT so
  locking scope is obvious.
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator

from . import config

# How long a writer waits behind another instance's lock before failing.
BUSY_TIMEOUT_MS = 15_000

_SCHEMA = """
CREATE TABLE IF NOT EXISTS blobs (
    sha256          TEXT PRIMARY KEY,
    size_bytes      INTEGER NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('ready', 'deleting')),
    quarantine_path TEXT
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id    TEXT PRIMARY KEY,
    sha256      TEXT NOT NULL,
    size_bytes  INTEGER NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('active', 'retired')),
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    retired_at  TEXT
);

CREATE INDEX IF NOT EXISTS idx_batches_sha256 ON batches(sha256);

-- One row per blob currently scheduled for / in the middle of garbage
-- collection.  Surviving rows after a crash make GC resumable.
CREATE TABLE IF NOT EXISTS gc_queue (
    sha256     TEXT PRIMARY KEY,
    enqueued_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
"""

_local = threading.local()
# Every live connection handed out, across all threads in the process.
# Connections live for the whole process; reset_connections() drains this.
_live_conns: set[sqlite3.Connection] = set()
_all_conns_lock = threading.Lock()


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(
        config.DB_PATH,
        timeout=BUSY_TIMEOUT_MS / 1000.0,
        isolation_level=None,  # autocommit; transactions are explicit
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    # WAL is persistent once set; doing it on every connection is harmless
    # and covers a freshly created database on the shared volume.
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    with _all_conns_lock:
        _live_conns.add(conn)
    return conn


def get_conn() -> sqlite3.Connection:
    conn = getattr(_local, "conn", None)
    # A reset_connections() call (e.g. test relocating the data dir)
    # closes the shared connection; every thread then reconnects.
    if conn is None or conn not in _live_conns:
        conn = _connect()
        _local.conn = conn
    return conn


def reset_connections() -> None:
    """Close every connection in every thread (test/relocation support)."""
    with _all_conns_lock:
        conns = list(_live_conns)
        _live_conns.clear()
    for conn in conns:
        conn.close()
    if hasattr(_local, "conn"):
        del _local.conn


def init_db() -> None:
    config.ensure_layout()
    conn = get_conn()
    conn.executescript(_SCHEMA)


@contextmanager
def transaction() -> Iterator[sqlite3.Connection]:
    """Serialize a unit of work with an explicit IMMEDIATE transaction.

    BEGIN IMMEDIATE takes the WAL write lock up front, so two concurrent
    instances uploading the same batch cannot both pass the "does this
    batch already exist?" check.
    """
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
