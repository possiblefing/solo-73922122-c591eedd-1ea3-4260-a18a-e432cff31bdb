"""Functional tests for upload/download/dedup/retire/GC rules."""
from __future__ import annotations

import hashlib

from conftest import make_zip, upload


def test_upload_and_download_roundtrip(client):
    body = make_zip({"reads.fastq": b"ACGT" * 100})
    r = upload(client, "B001", body)
    assert r.status_code == 200, r.text
    payload = r.json()
    assert payload["sha256"] == hashlib.sha256(body).hexdigest()
    assert payload["reused_existing_content"] is False

    r = client.get("/batches/B001/archive")
    assert r.status_code == 200
    assert r.content == body
    assert r.headers["content-type"] == "application/zip"


def test_dedup_identical_content_stored_once(client, data_dir):
    body = make_zip({"a": b"same-bytes"})
    assert upload(client, "B100", body).status_code == 200
    r2 = upload(client, "B101", body)
    assert r2.status_code == 200

    # Exactly one physical original on disk for two batches.
    blob_files = list((data_dir / "blobs").iterdir())
    assert len(blob_files) == 1
    assert blob_files[0].name == hashlib.sha256(body).hexdigest()

    summary = client.get("/storage/summary").json()
    assert summary["blob_rows"] == 1
    assert summary["blob_files_on_disk"] == 1
    assert summary["active_batches"] == 2


def test_same_batch_same_content_reupload_reuses(client):
    body = make_zip({"x": b"1"})
    upload(client, "B200", body)
    r = upload(client, "B200", body)
    assert r.status_code == 200
    assert r.json()["reused_existing_content"] is True


def test_same_batch_different_content_rejected(client):
    upload(client, "B300", make_zip({"x": b"one"}))
    r = upload(client, "B300", make_zip({"x": b"two-different"}))
    assert r.status_code == 409
    # Original content must remain intact.
    got = client.get("/batches/B300/archive").content
    assert got == make_zip({"x": b"one"})


def test_non_zip_rejected(client):
    r = upload(client, "B400", b"this is definitely not a zip")
    assert r.status_code == 400


def test_size_limit_enforced(client):
    # Just over 2 MiB, wrapped in a valid zip.
    big = make_zip({"big.bin": b"Z" * (2 * 1024 * 1024)})
    assert len(big) <= 2 * 1024 * 1024  # compressed body itself is small
    # Use an incompressible payload so the archive really exceeds the cap.
    import os
    big = make_zip({"big.bin": os.urandom(2 * 1024 * 1024 + 1024)})
    assert len(big) > 2 * 1024 * 1024
    r = upload(client, "B500", big)
    assert r.status_code == 413


def test_retire_then_download_404_and_number_unusable(client):
    body = make_zip({"r": b"data"})
    upload(client, "B600", body)

    r = client.post("/batches/B600/retire")
    assert r.status_code == 200
    assert r.json()["status"] == "retired"

    assert client.get("/batches/B600/archive").status_code == 404
    # Retired numbers cannot be reused, even with identical content.
    r = upload(client, "B600", body)
    assert r.status_code == 409


def test_retire_unknown_batch_404(client):
    assert client.post("/batches/nope/retire").status_code == 404


def test_retire_is_idempotent(client):
    upload(client, "B700", make_zip({"k": b"v"}))
    assert client.post("/batches/B700/retire").status_code == 200
    r = client.post("/batches/B700/retire")
    assert r.status_code == 200
    assert r.json()["already_retired"] is True


def test_gc_only_reclaims_unreferenced_originals(client, data_dir):
    shared = make_zip({"shared": b"shared-content"})
    solo = make_zip({"solo": b"solo-content"})
    upload(client, "BA", shared)
    upload(client, "BB", shared)   # same original, two batches
    upload(client, "BC", solo)

    shared_hash = hashlib.sha256(shared).hexdigest()
    solo_hash = hashlib.sha256(solo).hexdigest()
    assert (data_dir / "blobs" / shared_hash).exists()
    assert (data_dir / "blobs" / solo_hash).exists()

    # Retire one sharing batch AND the solo batch: the shared original
    # must survive (BB still references it), the solo original is reaped.
    client.post("/batches/BA/retire")
    client.post("/batches/BC/retire")
    assert (data_dir / "blobs" / shared_hash).exists()
    assert not (data_dir / "blobs" / solo_hash).exists()
    summary = client.get("/storage/summary").json()
    assert summary["blob_rows"] == 1
    assert summary["blob_files_on_disk"] == 1

    # Retire the second sharing batch: now the shared original goes too.
    client.post("/batches/BB/retire")
    assert not (data_dir / "blobs" / shared_hash).exists()
    summary = client.get("/storage/summary").json()
    assert summary["blob_rows"] == 0
    assert summary["blob_files_on_disk"] == 0
    assert summary["retired_batches"] == 3


def test_batch_summaries_list_and_filter(client):
    upload(client, "S1", make_zip({"a": b"1"}))
    upload(client, "S2", make_zip({"b": b"22"}))
    client.post("/batches/S1/retire")

    all_batches = client.get("/batches").json()
    assert all_batches["total"] == 2
    active = client.get("/batches", params={"status": "active"}).json()
    assert {b["batch_id"] for b in active["batches"]} == {"S2"}
    retired = client.get("/batches", params={"status": "retired"}).json()
    assert {b["batch_id"] for b in retired["batches"]} == {"S1"}

    one = client.get("/batches/S1").json()
    assert one["status"] == "retired"
    assert one["retired_at"] is not None
    assert client.get("/batches/missing").status_code == 404


def test_healthz(client):
    assert client.get("/healthz").text == "ok"


def test_invalid_batch_id_rejected(client, zip_factory):
    for bad in ("", "a/b", "a b", "../escape", "x" * 129):
        r = upload(client, bad, zip_factory({"z": b"1"}))
        assert r.status_code in (400, 404), (bad, r.status_code)
        assert client.post(f"/batches/{bad}/retire").status_code in (400, 404)


def test_repeated_gc_runs_are_idempotent(client, data_dir):
    body = make_zip({"g": b"once"})
    upload(client, "G1", body)
    client.post("/batches/G1/retire")
    # Further GC runs over an empty, converged store must stay clean.
    for _ in range(3):
        r = client.post("/gc/run")
        assert r.status_code == 200
        assert r.json()["deleted_blobs"] == []
    summary = client.get("/storage/summary").json()
    assert summary["blob_rows"] == summary["blob_files_on_disk"] == 0
    assert summary["queued_for_gc"] == 0
