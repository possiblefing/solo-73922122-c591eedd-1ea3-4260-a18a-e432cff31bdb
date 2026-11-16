"""Minimal uvicorn launcher used by the multi-instance tests.

Each concurrency test spawns two of these as separate OS processes so
they have independent Python interpreters and SQLite connections - the
same topology as the two Docker Compose replicas sharing one volume.
"""
from __future__ import annotations

import os

import uvicorn


def main() -> None:
    uvicorn.run(
        "app.api:app",
        host="127.0.0.1",
        port=int(os.environ["ARCHIVE_TEST_PORT"]),
        log_level="warning",
    )


if __name__ == "__main__":
    main()
