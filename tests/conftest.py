"""Shared fixtures: every test gets an isolated data directory.

The tests import the real FastAPI app and drive it with TestClient;
the underlying code is the same code the two Docker replicas run.
"""
from __future__ import annotations

import io
import os
import zipfile

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    root = tmp_path / "data"
    root.mkdir()
    monkeypatch.setenv("ARCHIVE_DATA_DIR", str(root))

    # config captured module constants at import time; repoint them.
    from app import config
    monkeypatch.setattr(config, "DATA_DIR", root.resolve())
    monkeypatch.setattr(config, "BLOB_DIR", root.resolve() / "blobs")
    monkeypatch.setattr(config, "QUARANTINE_DIR", root.resolve() / "quarantine")
    monkeypatch.setattr(config, "TMP_DIR", root.resolve() / "tmp")
    monkeypatch.setattr(config, "DB_PATH", root.resolve() / "archives.db")
    config.ensure_layout()

    # Fresh per-thread connections pointed at the new DB path: close the
    # whole process-wide pool, including worker-thread locals.
    from app import db
    db.reset_connections()
    db.init_db()
    yield root.resolve()


@pytest.fixture
def client(data_dir):
    from app.api import app
    with TestClient(app) as c:
        yield c


def make_zip(files: dict[str, bytes]) -> bytes:
    """Build a real ZIP archive entirely in memory."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


@pytest.fixture
def zip_factory():
    return make_zip


def upload(client, batch_id: str, body: bytes):
    return client.post(
        f"/batches/{batch_id}/archive",
        files={"file": (f"{batch_id}.zip", body, "application/zip")},
    )
