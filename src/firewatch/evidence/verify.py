"""Read-only evidence and local artifact auditing; no provider, fitting or chain writes."""

import hashlib
import json
import re
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy.orm import Session

from firewatch.config import Settings
from firewatch.db.models import ModelVersion, SourceAsset
from firewatch.evidence.files import MAX_FILE_BYTES, MAX_TOTAL_BYTES, AuditFailure, ReadBudget  # noqa: F401
from firewatch.processing.features import AssetPin
from firewatch.schemas.api import ArtifactAudit, ArtifactCheck

MAX_PAYLOAD_BYTES = 32 * 1024 * 1024

MAX_REFERENCES = 256
MODEL_FILES = {"model.json", "thresholds.json", "feature-contract.json"}


def verify_payload(payload: bytes, expected_sha256: str) -> tuple[str, str]:
    """Hash the exact supplied bytes, including whitespace, for saved or copied payloads."""
    computed = hashlib.sha256(payload).hexdigest()
    return ("local_match" if computed == expected_sha256 else "local_mismatch", computed)


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON member")
        result[key] = value
    return result


def _json(payload: bytes) -> Any:
    def invalid(value: str) -> None:
        raise ValueError("Nonfinite JSON")

    return json.loads(payload, object_pairs_hook=_unique, parse_constant=invalid)


def _references(payload: bytes) -> tuple[list[AssetPin], tuple[UUID, str, str] | None]:
    value = _json(payload)
    if not isinstance(value, dict) or value.get("schema_version") != "evidence-v1":
        raise AuditFailure("unsupported_payload")
    model = None
    if value.get("canonicalization_version") == "evidence-json-v1":
        refs = value["source_checksums"]
        if value.get("model_id") is not None:
            meta = value["model"]
            identifier = UUID(meta["model_id"])
            checksum = meta["inference_manifest_sha256"]
            version = meta["scorer_version"]
            if (
                str(identifier) != value["model_id"]
                or version != value["scorer_version"]
                or not isinstance(checksum, str)
                or not re.fullmatch(r"[0-9a-f]{64}", checksum)
                or not isinstance(version, str)
                or not 1 <= len(version) <= 100
            ):
                raise ValueError("Invalid model reference")
            model = identifier, checksum, version
        elif value.get("model") is not None:
            raise ValueError("Inconsistent model reference")
    elif "canonicalization_version" not in value:
        # Legacy worker payloads carry the same source pins in the frozen enrichment.
        # They do not bind an inference manifest, so supervised legacy claims stay unsupported.
        if value.get("model_id") is not None:
            raise AuditFailure("unsupported_payload")
        pins = value["source_manifest"]["enrichment"]
        refs = pins["assets"] + [
            {"asset_id": p["asset_id"], "sha256": p["sha256"]} for p in pins["satellite_assets"]
        ]
    else:
        raise AuditFailure("unsupported_payload")
    if not isinstance(refs, list):
        raise ValueError("Expected reference list")
    if len(refs) + (model is not None) > MAX_REFERENCES:
        raise AuditFailure("reference_limit")
    unique: dict[UUID, AssetPin] = {}
    for raw in refs:
        pin = AssetPin.model_validate(raw)
        if pin.asset_id in unique and unique[pin.asset_id].sha256 != pin.sha256:
            raise ValueError("Conflicting source references")
        unique[pin.asset_id] = pin
    if not unique and model is None:
        raise AuditFailure("unsupported_payload")
    return sorted(unique.values(), key=lambda p: str(p.asset_id)), model


def _source(session: Session, pin: AssetPin, settings: Settings, budget: ReadBudget) -> None:
    asset = session.get(SourceAsset, pin.asset_id)
    if asset is None:
        raise AuditFailure("missing_record")
    if asset.content_hash != pin.sha256:
        raise AuditFailure("metadata_mismatch", mismatch=True)
    from firewatch.evidence.storage import source_bytes

    source_bytes(asset.storage_path, pin.sha256, settings, budget)


def _model(
    session: Session, ref: tuple[UUID, str, str], settings: Settings, budget: ReadBudget
) -> None:
    identifier, checksum, version = ref
    model = session.get(ModelVersion, identifier)
    if model is None:
        raise AuditFailure("missing_record")
    if model.artifact_hash != checksum or model.version != version:
        raise AuditFailure("metadata_mismatch", mismatch=True)
    # Historical evidence remains auditable after activation changes or retirement.
    supplied = Path(model.artifact_path)
    root = supplied if supplied.is_absolute() else settings.model_dir / supplied
    raw = budget.read(root / "manifest.json", [settings.model_dir], 65536)
    if hashlib.sha256(raw).hexdigest() != checksum:
        raise AuditFailure("checksum_mismatch", mismatch=True)
    try:
        manifest = _json(raw)
        if (
            manifest["schema_version"] != "inference-bundle-v1"
            or manifest["scorer_version"] != version
            or set(manifest["files"]) != MODEL_FILES
        ):
            raise ValueError("Invalid inference manifest")
        for name in sorted(MODEL_FILES):
            entry = manifest["files"][name]
            if (
                type(entry["size"]) is not int
                or entry["size"] < 0
                or not isinstance(entry["sha256"], str)
                or not re.fullmatch(r"[0-9a-f]{64}", entry["sha256"])
            ):
                raise ValueError("Invalid file entry")
            content = budget.read(root / name, [settings.model_dir], 32 * 1024 * 1024)
            if (
                len(content) != entry["size"]
                or hashlib.sha256(content).hexdigest() != entry["sha256"]
            ):
                raise AuditFailure("checksum_mismatch", mismatch=True)
    except (ValueError, KeyError, TypeError, RecursionError):
        raise AuditFailure("invalid_manifest", mismatch=True) from None


def audit_artifacts(
    session: Session, payload: bytes, expected_sha256: str, settings: Settings
) -> ArtifactAudit:
    """Verify pins from original evidence, never public projections or caller-supplied paths."""
    if verify_payload(payload, expected_sha256)[0] != "local_match":
        return ArtifactAudit(status="unavailable", reason="payload_mismatch")
    if len(payload) > MAX_PAYLOAD_BYTES:
        return ArtifactAudit(status="unavailable", reason="reference_limit")
    try:
        sources, model = _references(payload)
    except AuditFailure as exc:
        return ArtifactAudit.model_validate({"status": "unavailable", "reason": exc.reason})
    except (ValueError, KeyError, TypeError, AttributeError, RecursionError, ValidationError):
        return ArtifactAudit(status="unavailable", reason="invalid_references")
    budget = ReadBudget()
    items: list[ArtifactCheck] = []
    references = [("source", pin.asset_id, pin) for pin in sources]
    for kind, identifier, pin in references:
        try:
            _source(session, pin, settings, budget)
            item = dict(kind=kind, artifact_id=identifier, status="match", reason="checksum_match")
        except AuditFailure as exc:
            item = dict(
                kind=kind,
                artifact_id=identifier,
                status="mismatch" if exc.mismatch else "unavailable",
                reason=exc.reason,
            )
        items.append(ArtifactCheck.model_validate(item))
    if model is not None:
        try:
            _model(session, model, settings, budget)
            status, reason = "match", "checksum_match"
        except AuditFailure as exc:
            status, reason = "mismatch" if exc.mismatch else "unavailable", exc.reason
        items.append(
            ArtifactCheck.model_validate(
                dict(kind="model", artifact_id=model[0], status=status, reason=reason)
            )
        )
    states = {item.status for item in items}
    aggregate = (
        "mismatch"
        if "mismatch" in states
        else "verified"
        if states == {"match"}
        else "unavailable"
        if states == {"unavailable"}
        else "partial"
    )
    return ArtifactAudit.model_validate(
        dict(
            status=aggregate,
            reason="completed",
            items=items,
            scope="referenced_sources_and_local_model"
            if settings.supabase_storage_enabled
            else "referenced_local_files",
        )
    )
