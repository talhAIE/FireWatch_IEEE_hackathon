"""Transactional approval registry and frozen worker references; no training or inference I/O."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from firewatch.config import Settings
from firewatch.db.models import ModelActivation, ModelPromotion, ModelVersion, Region
from firewatch.ml.inference_bundle import ApprovedScorer
from firewatch.ml.registry_artifacts import PromotionCandidate, managed_path, verify_managed_release
from firewatch.schemas.common import ModelStatus, RunMode

REGISTRY_LOCK = 0x46574D4F444C


class ModelRegistryError(ValueError):
    """Safe fixed-message registry/approval failure; never substitute a baseline."""


class ModelScoringError(ValueError):
    """Required inputs or approved artifact failed at scoring; no scores are published."""


def selected_version(session: Session, settings: Settings, *, mode: RunMode | None = None) -> str:
    configured = (
        settings.live_scorer
        if mode == RunMode.LIVE and settings.live_scorer is not None
        else settings.active_scorer
    )
    if configured != "registry":
        return configured
    active = session.get(ModelActivation, "default")
    if active is None:
        raise ModelRegistryError("No active approved model; configure an explicit baseline")
    model = session.get(ModelVersion, active.active_model_id)
    if model is None or model.status != ModelStatus.APPROVED:
        raise ModelRegistryError("Active model is not approved")
    return model.version


def resolve_approved(
    session: Session,
    settings: Settings,
    version: str,
    *,
    region: Region | None = None,
    mode: RunMode | None = None,
) -> ApprovedScorer:
    model = session.scalar(
        select(ModelVersion).where(ModelVersion.version == version).with_for_update(read=True)
    )
    if (
        model is None
        or model.status != ModelStatus.APPROVED
        or model.feature_version != settings.feature_schema_version
        or not isinstance(model.training_manifest, dict)
        or model.training_manifest.get("schema_version") != "model-registry-v1"
    ):
        raise ModelRegistryError("Requested model is unavailable or not registry-approved")
    try:
        metadata = model.training_manifest
        if region is not None:
            bbox = metadata["scope"]["bbox"]
            if (
                region.name != metadata["scope"]["region"]
                or region.west < bbox["west"]
                or region.east > bbox["east"]
                or region.south < bbox["south"]
                or region.north > bbox["north"]
            ):
                raise ModelRegistryError("Requested region is outside reviewed model scope")
        if mode is not None and mode.value not in metadata["scope"]["modes"]:
            raise ModelRegistryError("Requested run mode is outside reviewed model scope")
        path = managed_path(settings.model_dir, model.artifact_path)
        return ApprovedScorer(
            model_id=model.id,
            scorer_version=model.version,
            artifact_path=path,
            manifest_sha256=model.artifact_hash,
            approval_status="approved",
            approval_reference=metadata["approval_reference"],
        )
    except ModelRegistryError:
        raise
    except (ValueError, KeyError, TypeError):
        raise ModelRegistryError("Approved model metadata or path is invalid") from None


def promote(
    session: Session, model_dir: Path, candidate: PromotionCandidate, now: datetime | None = None
) -> dict[str, Any]:
    """Caller owns the transaction. All expensive offline validation precedes this call."""
    session.execute(select(func.pg_advisory_xact_lock(REGISTRY_LOCK)))
    active = session.get(ModelActivation, "default", with_for_update=True)
    model = session.scalar(select(ModelVersion).where(ModelVersion.version == candidate.version))
    event_id = uuid4()
    if model is not None:
        if (
            model.status != ModelStatus.APPROVED
            or model.artifact_hash != candidate.bundle_sha256
            or model.training_manifest.get("release_sha256") != candidate.release_sha256
        ):
            raise ModelRegistryError("Version already exists with a different approval or artifact")
        if active is not None and active.active_model_id == model.id:
            return dict(version=model.version, model_id=str(model.id), already_active=True)
    else:
        relative = (candidate.release_root / "bundle").relative_to(model_dir.resolve()).as_posix()
        model = ModelVersion(
            id=uuid4(),
            version=candidate.version,
            artifact_path=relative,
            artifact_hash=candidate.bundle_sha256,
            feature_version="features-v1",
            probability_supported=candidate.probability_supported,
            status=ModelStatus.APPROVED,
            target_definition=candidate.target,
            metrics=candidate.metrics,
            training_manifest=dict(
                schema_version="model-registry-v1",
                source_evaluation_sha256=candidate.evaluation_sha256,
                release_sha256=candidate.release_sha256,
                approval_reference=str(event_id),
                review=candidate.review.model_dump(mode="json"),
                scope={
                    **candidate.target,
                    "modes": [m.value for m in candidate.review.supported_modes],
                },
            ),
        )
        session.add(model)
        session.flush()
    event = ModelPromotion(
        id=event_id,
        model_id=model.id,
        previous_model_id=active.active_model_id if active else None,
        action="promote",
        reviewer=candidate.review.reviewer,
        rationale=candidate.review.rationale,
        created_at=now or datetime.now(UTC),
        release_sha256=candidate.release_sha256,
    )
    session.add(event)
    if active is None:
        active = ModelActivation(
            name="default", active_model_id=model.id, previous_model_id=None, revision=1
        )
        session.add(active)
    else:
        active.previous_model_id, active.active_model_id = active.active_model_id, model.id
        active.revision += 1
    session.flush()
    return dict(
        version=model.version,
        model_id=str(model.id),
        already_active=False,
        activation_revision=active.revision,
    )


def rollback(
    session: Session,
    settings: Settings,
    *,
    version: str | None,
    reviewer: str,
    rationale: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    if not reviewer.strip() or len(reviewer) > 100 or not 10 <= len(rationale.strip()) <= 2000:
        raise ModelRegistryError("Rollback requires a named operator and reason")
    session.execute(select(func.pg_advisory_xact_lock(REGISTRY_LOCK)))
    active = session.get(ModelActivation, "default", with_for_update=True)
    if active is None:
        raise ModelRegistryError("No active model exists for rollback")
    model = (
        session.scalar(select(ModelVersion).where(ModelVersion.version == version))
        if version
        else session.get(ModelVersion, active.previous_model_id)
        if active.previous_model_id
        else None
    )
    if model is None or model.id == active.active_model_id:
        raise ModelRegistryError("No distinct previous approved version is available")
    reference = resolve_approved(session, settings, model.version)
    verify_managed_release(
        reference.artifact_path.parent, model.training_manifest["release_sha256"]
    )
    prior = active.active_model_id
    active.active_model_id, active.previous_model_id = model.id, prior
    active.revision += 1
    session.add(
        ModelPromotion(
            id=uuid4(),
            model_id=model.id,
            previous_model_id=prior,
            action="rollback",
            reviewer=reviewer,
            rationale=rationale,
            created_at=now or datetime.now(UTC),
            release_sha256=model.training_manifest["release_sha256"],
        )
    )
    session.flush()
    return dict(version=model.version, model_id=str(model.id), activation_revision=active.revision)
