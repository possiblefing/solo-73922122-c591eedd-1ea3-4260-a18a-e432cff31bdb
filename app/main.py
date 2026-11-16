"""Uvicorn entry point: ``python -m app.main`` or the container CMD."""
from __future__ import annotations

import os

import uvicorn

from .api import app  # noqa: F401  (imported for app discovery)


def main() -> None:
    uvicorn.run(
        "app.api:app",
        host=os.environ.get("HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
        # One process per container; the compose file runs two replicas.
        # A modest thread pool keeps slow streaming downloads from
        # starving the (short, lock-bound) mutating endpoints.
        workers=int(os.environ.get("WEB_CONCURRENCY", "1")),
        log_level=os.environ.get("LOG_LEVEL", "info"),
    )


if __name__ == "__main__":
    main()
