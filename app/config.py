"""Runtime configuration.

All state lives under a single data directory so that several API
instances can share one volume (see docker-compose.yml).
"""
from __future__ import annotations

import os
from pathlib import Path

DATA_DIR = Path(os.environ.get("ARCHIVE_DATA_DIR", "/data")).resolve()

# A whole batch upload must fit in 2 MiB.
MAX_UPLOAD_BYTES = 2 * 1024 * 1024

# Sub-directories of the shared volume.
BLOB_DIR = DATA_DIR / "blobs"          # content-addressed originals (sha256)
QUARANTINE_DIR = DATA_DIR / "quarantine"  # blobs being removed by the GC
TMP_DIR = DATA_DIR / "tmp"             # in-flight upload parts
DB_PATH = DATA_DIR / "archives.db"


def ensure_layout() -> None:
    for directory in (DATA_DIR, BLOB_DIR, QUARANTINE_DIR, TMP_DIR):
        directory.mkdir(parents=True, exist_ok=True)
