"""Exact-byte tamper detection and bounded, source-pinned artifact auditing."""

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
from evidence_factory import example

from firewatch.config import Settings
from firewatch.db.models import ModelVersion, SourceAsset
from firewatch.evidence.payload import evidence_content
from firewatch.evidence.verify import (
    MAX_FILE_BYTES,
    AuditFailure,
    ReadBudget,
    audit_artifacts,
    verify_payload,
)


def sha(content):
    return hashlib.sha256(content).hexdigest()


@pytest.fixture
def audit(tmp_path):
    settings = Settings(
        database_url="postgresql+psycopg://test:password@localhost:1/firewatch_test",
        admin_api_token="test-only-operator-" + "x" * 40,
        raw_data_dir=tmp_path / "raw",
        replay_bundle_dir=tmp_path / "replays",
        model_dir=tmp_path / "models",
        _env_file=None,
    )
    settings.raw_data_dir.mkdir()
    settings.model_dir.mkdir()
    payload = json.loads(evidence_content(example())[0])
    records = {}
    for i, ref in enumerate(payload["source_checksums"]):
        path = settings.raw_data_dir / f"source-{i}.bin"
        path.write_bytes(f"synthetic source {i}".encode())
        ref["sha256"] = sha(path.read_bytes())
        records[SourceAsset, UUID(ref["asset_id"])] = SimpleNamespace(
            content_hash=ref["sha256"], storage_path=str(path)
        )
    session = MagicMock()
    session.get.side_effect = lambda kind, key: records.get((kind, key))
    return settings, payload, records, session


def run(audit):
    settings, payload, _, session = audit
    raw = json.dumps(payload).encode()
    return audit_artifacts(session, raw, sha(raw), settings)


def test_copied_payload_tampering_and_whitespace_never_rewrite_original():
    raw, expected = evidence_content(example())
    copied = json.loads(raw)
    copied["priority_score"] = "999"
    assert verify_payload(raw, expected) == ("local_match", expected)
    assert verify_payload(json.dumps(copied).encode(), expected)[0] == "local_mismatch"
    assert verify_payload(raw + b"\n", expected)[0] == "local_mismatch"
    assert sha(raw) == expected


def test_audit_all_source_bytes_and_no_writes_or_model_loading(audit, monkeypatch):
    import socket

    monkeypatch.setattr(socket, "create_connection", lambda *a, **k: pytest.fail("network I/O"))
    result = run(audit)
    assert result.status == "verified"
    assert all(item.status == "match" for item in result.items)
    session = audit[3]
    session.add.assert_not_called()
    session.execute.assert_not_called()
    session.commit.assert_not_called()
    encoded = result.model_dump_json()
    assert str(audit[0].raw_data_dir) not in encoded and "storage_path" not in encoded


def test_portable_source_mirror_ignores_foreign_locators_and_checks_exact_bytes(audit, tmp_path):
    settings, _, records, _ = audit
    mirror = tmp_path / "portable"
    mirror.mkdir()
    settings.source_artifact_dir = mirror
    for record in records.values():
        content = Path(record.storage_path).read_bytes()
        (mirror / f"{record.content_hash}.bin").write_bytes(content)
        record.storage_path = r"Z:\another-workstation\missing.bin"
    assert run(audit).status == "verified"
    first = next(iter(records.values()))
    (mirror / f"{first.content_hash}.bin").write_bytes(b"changed bytes")
    assert any(item.reason == "checksum_mismatch" for item in run(audit).items)
    (mirror / f"{first.content_hash}.bin").unlink()
    assert any(item.reason == "missing_file" for item in run(audit).items)


def test_portable_mirror_does_not_override_database_checksum(audit, tmp_path):
    settings, _, records, _ = audit
    settings.source_artifact_dir = tmp_path
    next(iter(records.values())).content_hash = "0" * 64
    assert any(item.reason == "metadata_mismatch" for item in run(audit).items)


def test_new_live_source_can_be_verified_alongside_frozen_mirror(audit, tmp_path):
    settings, _, records, _ = audit
    settings.source_artifact_dir = tmp_path / "mirror"
    settings.source_artifact_dir.mkdir()
    # The database locators already point into RAW_DATA_DIR; no mirror copy yet.
    assert run(audit).status == "verified"
    first = next(iter(records.values()))
    mirror = settings.source_artifact_dir / f"{first.content_hash}.bin"
    mirror.write_bytes(b"tampered mirror")
    # A present bad mirror must never be hidden by a good fallback file.
    assert any(item.reason == "checksum_mismatch" for item in run(audit).items)


@pytest.mark.parametrize(
    "change,expected,reason",
    [
        ("tamper", "mismatch", "checksum_mismatch"),
        ("metadata", "mismatch", "metadata_mismatch"),
        ("missing", "partial", "missing_file"),
        ("record", "partial", "missing_record"),
        ("outside", "partial", "unsafe_path"),
        ("directory", "partial", "unsafe_path"),
        ("traversal", "partial", "unsafe_path"),
        ("oversize", "partial", "size_limit"),
    ],
)
def test_source_failures_remain_distinct(audit, tmp_path, change, expected, reason):
    _, _, records, _ = audit
    key, asset = next(iter(records.items()))
    path = Path(asset.storage_path)
    if change == "tamper":
        path.write_bytes(b"tampered source")
    elif change == "metadata":
        asset.content_hash = "f" * 64
    elif change == "missing":
        path.unlink()
    elif change == "record":
        records.pop(key)
    elif change == "outside":
        outside = tmp_path / "private.bin"
        outside.write_bytes(path.read_bytes())
        asset.storage_path = str(outside)
    elif change == "directory":
        asset.storage_path = str(path.parent)
    elif change == "traversal":
        asset.storage_path = str(path.parent / ".." / "raw" / path.name)
    elif change == "oversize":
        with path.open("wb") as stream:
            stream.truncate(MAX_FILE_BYTES + 1)
    result = run(audit)
    assert result.status == expected
    assert any(item.reason == reason for item in result.items)


def test_payload_mismatch_does_not_follow_any_references(audit):
    settings, _, _, session = audit
    result = audit_artifacts(session, b"tampered", "a" * 64, settings)
    assert result.reason == "payload_mismatch" and result.items == []
    session.get.assert_not_called()


@pytest.mark.parametrize(
    "raw,reason",
    [
        (b"not JSON", "invalid_references"),
        (b'{"schema_version":"unknown"}', "unsupported_payload"),
        (b'{"schema_version":"evidence-v1"}', "invalid_references"),
        (b'{"schema_version":"evidence-v1","schema_version":"unknown"}', "invalid_references"),
        (b'{"value":NaN}', "invalid_references"),
    ],
)
def test_unknown_and_ambiguous_payloads_are_not_verified(audit, raw, reason):
    settings, _, _, session = audit
    result = audit_artifacts(session, raw, sha(raw), settings)
    assert result.status == "unavailable" and result.reason == reason
    session.get.assert_not_called()


def test_conflicting_pins_and_reference_limit(audit):
    payload = audit[1]
    original = payload["source_checksums"][0]
    payload["source_checksums"].append({**original, "sha256": "f" * 64})
    assert run(audit).reason == "invalid_references"
    payload["source_checksums"] = [original] * 257
    assert run(audit).reason == "reference_limit"


def test_all_missing_and_mismatch_precedence(audit):
    for asset in audit[2].values():
        Path(asset.storage_path).unlink()
    assert run(audit).status == "unavailable"
    first = next(iter(audit[2].values()))
    Path(first.storage_path).write_bytes(b"tampered")
    assert run(audit).status == "mismatch"


def test_legacy_pins_are_verified_without_recanonicalization(audit):
    refs = audit[1]["source_checksums"]
    audit[1].clear()
    audit[1].update(
        schema_version="evidence-v1",
        source_manifest={"enrichment": {"assets": refs, "satellite_assets": []}},
    )
    assert run(audit).status == "verified"
    audit[1]["model_id"] = str(uuid4())
    assert run(audit).reason == "unsupported_payload"


def test_replay_paths_and_budget(audit):
    settings, _, records, _ = audit
    settings.replay_bundle_dir.mkdir()
    asset = next(iter(records.values()))
    original = Path(asset.storage_path)
    copied = settings.replay_bundle_dir / "source.bin"
    copied.write_bytes(original.read_bytes())
    asset.storage_path = str(copied)
    assert run(audit).status == "verified"
    with pytest.raises(AuditFailure) as exc:
        ReadBudget(remaining=1).read(copied, [settings.replay_bundle_dir])
    assert exc.value.reason == "size_limit"


def model_bundle(audit):
    settings, payload, records, _ = audit
    root = settings.model_dir / "retired-v1"
    root.mkdir()
    files = {}
    for name in ("model.json", "thresholds.json", "feature-contract.json"):
        # Deliberately non-loadable bytes: an audit must hash, never deserialize an estimator.
        content = b"synthetic bytes for checksum-only verification"
        (root / name).write_bytes(content)
        files[name] = {"size": len(content), "sha256": sha(content)}
    manifest = dict(
        schema_version="inference-bundle-v1", scorer_version="forest-test-v1", files=files
    )
    raw = json.dumps(manifest).encode()
    (root / "manifest.json").write_bytes(raw)
    identifier = uuid4()
    payload.update(
        model_id=str(identifier),
        scorer_version="forest-test-v1",
        model=dict(
            model_id=str(identifier),
            scorer_version="forest-test-v1",
            inference_manifest_sha256=sha(raw),
        ),
    )
    model = SimpleNamespace(
        artifact_path="retired-v1",
        artifact_hash=sha(raw),
        version="forest-test-v1",
        status="retired",
    )
    records[ModelVersion, identifier] = model
    return root, model, manifest


def test_model_audit_checks_every_pinned_file_without_requiring_current_activation(audit):
    root, _, _ = model_bundle(audit)
    assert run(audit).status == "verified"
    (root / "model.json").write_bytes(b"tampered")
    result = run(audit)
    assert result.status == "mismatch"
    assert result.items[-1].kind == "model" and result.items[-1].reason == "checksum_mismatch"


@pytest.mark.parametrize("change", ["missing", "metadata", "manifest", "unsafe", "invalid"])
def test_model_failures(audit, change):
    root, model, manifest = model_bundle(audit)
    if change == "missing":
        (root / "thresholds.json").unlink()
    elif change == "metadata":
        model.artifact_hash = "f" * 64
    elif change == "manifest":
        (root / "manifest.json").write_bytes(b"tampered")
    elif change == "unsafe":
        model.artifact_path = "../private"
    elif change == "invalid":
        manifest["files"]["../private"] = manifest["files"].pop("model.json")
        raw = json.dumps(manifest).encode()
        (root / "manifest.json").write_bytes(raw)
        model.artifact_hash = sha(raw)
        audit[1]["model"]["inference_manifest_sha256"] = sha(raw)
    result = run(audit)
    assert result.items[-1].status == (
        "unavailable" if change in {"missing", "unsafe"} else "mismatch"
    )


def test_symlink_source_is_never_followed(audit, tmp_path):
    asset = next(iter(audit[2].values()))
    target = Path(asset.storage_path)
    link = target.parent / "symlink.bin"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("Creating symlinks requires Windows developer mode or privilege")
    asset.storage_path = str(link)
    assert any(item.reason == "unsafe_path" for item in run(audit).items)


@pytest.mark.parametrize("method", ["is_symlink", "is_junction"])
def test_reparse_point_guard_before_open_without_os_privileges(audit, monkeypatch, method):
    target = Path(next(iter(audit[2].values())).storage_path)
    original = getattr(Path, method)
    monkeypatch.setattr(Path, method, lambda path: path == target or original(path))
    with pytest.raises(AuditFailure) as exc:
        ReadBudget().read(target, [audit[0].raw_data_dir])
    assert exc.value.reason == "unsafe_path"


def test_read_failure_does_not_leak_exception_or_private_path(audit, monkeypatch):
    target = Path(next(iter(audit[2].values())).storage_path)
    original = Path.open

    def denied(path, *args, **kwargs):
        if path == target:
            raise PermissionError("PRIVATE_FILE_CREDENTIAL")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", denied)
    result = run(audit)
    assert result.status == "partial"
    assert any(item.reason == "read_failed" for item in result.items)
    assert "PRIVATE_FILE_CREDENTIAL" not in result.model_dump_json()


def test_stream_growth_after_stat_cannot_bypass_read_budget(audit, monkeypatch):
    from io import BytesIO

    target = Path(next(iter(audit[2].values())).storage_path)
    # Stat sees the small original file, but the opened stream has grown.
    monkeypatch.setattr(Path, "open", lambda *args, **kwargs: BytesIO(b"x" * 20))
    with pytest.raises(AuditFailure) as exc:
        ReadBudget(remaining=10).read(target, [audit[0].raw_data_dir])
    assert exc.value.reason == "size_limit"
