"""Crash-recovery worker.

Runs one GC sweep against an existing data directory and hard-exits
(``os._exit``) at a chosen checkpoint so no ``finally``/atexit cleanup
can hide the interrupted state.  Invoked as a separate process by
``test_recovery.py`` to prove reclamation resumes across restarts.
"""
from __future__ import annotations

import os
import sys

# Trusted project code: make the package importable when run as a file.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import config, db, storage  # noqa: E402


def main() -> int:
    data_dir = sys.argv[1]
    hook = sys.argv[2] if len(sys.argv) > 2 else ""

    os.environ["ARCHIVE_DATA_DIR"] = data_dir
    config.DATA_DIR = __import__("pathlib").Path(data_dir).resolve()
    config.BLOB_DIR = config.DATA_DIR / "blobs"
    config.QUARANTINE_DIR = config.DATA_DIR / "quarantine"
    config.TMP_DIR = config.DATA_DIR / "tmp"
    config.DB_PATH = config.DATA_DIR / "archives.db"
    config.ensure_layout()

    db.init_db()
    if hook:
        storage.CRASH_HOOK = hook
    storage.run_gc()
    return 0


if __name__ == "__main__":
    sys.exit(main())
