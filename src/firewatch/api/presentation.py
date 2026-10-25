"""Public projections, immutable-run timestamps and honest scorer/anchor metadata."""

import hashlib
import json
import math
import re
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any, Protocol

from pydantic import JsonValue
from sqlalchemy import select
from sqlalchemy.orm import Session

from firewatch.config import Settings
from firewatch.db.models import Alert, Anchor, AnchorOutbox, ModelVersion, Run
from firewatch.evidence.anchor import alert_key
from firewatch.evidence.verify import audit_artifacts, verify_payload
from firewatch.ml.baselines import BASELINE_VERSIONS
from firewatch.ml.inference_bundle import load_bundle as load_inference_bundle
from firewatch.ml.registry import ModelRegistryError, selected_version
from firewatch.ml.registry_artifacts import managed_path
from firewatch.repositories.reads import run_times
from firewatch.schemas.api import (
    AnchorView,
    Freshness,
    HotspotView,
    ModelMetadata,
    RunContext,
    RunView,
    Verification,
)
from firewatch.schemas.common import (
    AnchorStatus,
    ModelStatus,
    RunMode,
    ScoreKind,
    Stage,
    StageStatus,
)

SENSITIVE_KEYS = {
    "password",
    "token",
    "api_key",
    "map_key",
    "mapkey",
    "apikey",
    "authorization",
    "secret",
    "private_key",
    "chain_private_key",
    "database_url",
    "admin_api_token",
    "storage_path",
    "artifact_path",
    "request_metadata",
    "request_url",
    "url",
    "raw_values",
    "input_asset_manifest",
    "training_manifest",
    "raw_transaction",
    "signed_transaction",
    "raw_path",
    "path",
}


def public_json(value: Any, depth: int = 0) -> JsonValue:
    """Public projection only. Original evidence bytes are never modified."""
    if depth > 12:
        return None
    if value is None or isinstance(value, bool | int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, str):
        if (
            re.search(r"(?i)(?:https?://|postgres(?:ql)?(?:\+psycopg)?://|file://|[a-z]:\\)", value)
            or re.search(r"(?i)(?:api_key|map_key|token|password|secret)\s*[:=]", value)
            or value.startswith(("/", "\\"))
        ):
            return "[redacted]"
        return value[:4000]
    if isinstance(value, list):
        return [public_json(item, depth + 1) for item in value[:2000]]
    if isinstance(value, dict):
        return {
            str(key): public_json(item, depth + 1)
            for key, item in list(value.items())[:1000]
            if str(key).lower().replace("-", "_") not in SENSITIVE_KEYS
        }
    return None


def public_object(value: Any) -> dict[str, JsonValue]:
    result = public_json(value)
    return result if isinstance(result, dict) else {}


def safe_text(value: Any, default: str = "") -> str:
    result = public_json(value)
    return result if isinstance(result, str) and result != "[redacted]" else default


def aware_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return result.astimezone(UTC) if result.tzinfo is not None else None
    except ValueError:
        return None


def context(
    session: Session,
    run: Run,
    now: datetime,
    settings: Settings,
    *,
    times: tuple[datetime | None, datetime | None] | None = None,
) -> RunContext:
    observed, scored = run_times(session, run) if times is None else times
    reference = run.as_of if run.mode == RunMode.REPLAY else now
    saved_manifest = run.input_asset_manifest if isinstance(run.input_asset_manifest, dict) else {}
    manifest = saved_manifest.get("freshness", {})
    manifest = manifest if isinstance(manifest, dict) else {}
    weather = aware_timestamp(manifest.get("latest_weather_valid_at"))
    retrieved = aware_timestamp(manifest.get("latest_retrieved_at"))
    flags: list[str] = []
    if weather is not None and weather > run.as_of:
        weather = None
        flags.append("future_weather_metadata_ignored")
    if weather is None:
        flags.append("weather_freshness_unavailable")
    if observed is None:
        flags.append("no_snapshot_observations")
    observation_age = max(0.0, (reference - observed).total_seconds() / 3600) if observed else None
    weather_age = max(0.0, (reference - weather).total_seconds() / 3600) if weather else None
    return RunContext(
        run_id=run.id,
        mode=run.mode,
        as_of=run.as_of,
        data_completeness=run.completeness,
        is_historical=run.mode == RunMode.REPLAY,
        freshness=Freshness(
            evaluated_at=now,
            reference_time=reference,
            reference_kind="historical_cutoff" if run.mode == RunMode.REPLAY else "current_time",
            latest_detection_at=observed,
            latest_weather_valid_at=weather,
            latest_retrieved_at=retrieved,
            latest_scored_at=scored,
            completed_at=run.completed_at,
            observation_age_hours=observation_age,
            weather_age_hours=weather_age,
            observations_stale=observation_age > settings.detection_max_age_hours
            if observation_age is not None
            else None,
            weather_stale=weather_age > settings.weather_max_age_hours
            if weather_age is not None
            else None,
            wall_clock_observation_age_hours=max(0.0, (now - observed).total_seconds() / 3600)
            if observed
            else None,
            wall_clock_weather_age_hours=max(0.0, (now - weather).total_seconds() / 3600)
            if weather
            else None,
            observations_currently_stale=(now - observed).total_seconds()
            > settings.detection_max_age_hours * 3600
            or observed > now
            if observed
            else None,
            weather_currently_stale=(now - weather).total_seconds()
            > settings.weather_max_age_hours * 3600
            or weather > now
            if weather
            else None,
            quality_flags=flags,
        ),
    )


def stages(run: Run) -> dict[Stage, StageStatus]:
    result: dict[Stage, StageStatus] = {}
    for name, value in run.stage_status.items():
        if name in Stage._value2member_map_ and value in StageStatus._value2member_map_:
            result[Stage(name)] = StageStatus(value)
    return result


def run_view(
    session: Session,
    run: Run,
    now: datetime,
    settings: Settings,
    *,
    times: tuple[datetime | None, datetime | None] | None = None,
) -> RunView:
    return RunView(
        **context(session, run, now, settings, times=times).model_dump(),
        region_id=run.region_id,
        status=run.status,
        config_hash=run.config_hash,
        scorer_version=run.scorer_version,
        feature_version=run.feature_version,
        stages=stages(run),
        created_at=run.created_at,
        started_at=run.started_at,
        completed_at=run.completed_at,
    )


def hotspot(row: Any) -> HotspotView:
    snapshot, score, features, alert_id = row
    explanation = public_object(score.explanation)
    reasons = explanation.get("reasons", [])
    reasons = (
        [safe_text(item) for item in reasons[:3] if isinstance(item, str)]
        if isinstance(reasons, list)
        else []
    )
    flags = features.lineage.get("quality_flags", [])
    flags = (
        [safe_text(item) for item in flags if isinstance(item, str)]
        if isinstance(flags, list)
        else []
    )
    flags.extend(
        name + "_missing" for name, missing in features.missing_flags.items() if missing is True
    )
    warnings = explanation.get("warnings", [])
    if isinstance(warnings, list):
        flags.extend(safe_text(item) for item in warnings if isinstance(item, str))
    return HotspotView(
        snapshot_id=snapshot.id,
        latitude=snapshot.latitude,
        longitude=snapshot.longitude,
        member_count=snapshot.member_count,
        latest_detection_at=snapshot.latest_detection_at,
        frp_max_mw=snapshot.frp_max_mw,
        frp_sum_mw=snapshot.frp_sum_mw,
        priority=score.priority,
        priority_score=score.priority_score,
        score_kind=score.score_kind,
        escalation_probability=score.escalation_probability,
        scorer_version=score.scorer_version,
        feature_version=features.feature_version,
        scored_at=score.scored_at,
        reasons=reasons,
        quality_flags=sorted(set(flags)),
        alert_id=alert_id,
    )


class ArtifactInspector:
    """Bounded cache: checks bytes without ever loading a serialized model."""

    def __init__(self) -> None:
        self._cache: dict[tuple[str, str, int, int], bool] = {}
        self._details: dict[tuple[str, str], dict[str, Any]] = {}
        self._lock = Lock()

    def ready(self, model: ModelVersion, root: Path) -> bool:
        try:
            base = root.resolve()
            supplied = Path(model.artifact_path)
            path = (supplied if supplied.is_absolute() else base / supplied).resolve()
            if (
                isinstance(model.training_manifest, dict)
                and model.training_manifest.get("schema_version") == "model-registry-v1"
            ):
                path = managed_path(root, model.artifact_path)
                files = [
                    path / name
                    for name in (
                        "manifest.json",
                        "model.json",
                        "thresholds.json",
                        "feature-contract.json",
                    )
                ]
                fingerprint = tuple(
                    (file.stat().st_mtime_ns, file.stat().st_size) for file in files
                )
                key = (
                    str(path),
                    model.artifact_hash,
                    hash(fingerprint),
                    sum(v[1] for v in fingerprint),
                )
                with self._lock:
                    if key not in self._cache:
                        bundle = load_inference_bundle(path, model.artifact_hash, model.version)
                        if len(self._cache) >= 64:
                            self._cache.clear()
                            self._details.clear()
                        self._details[str(path), model.artifact_hash] = bundle.describe()
                        self._cache[key] = True
                    return self._cache[key]
            if not path.is_relative_to(base) or not path.is_file():
                return False
            stat = path.stat()
            if stat.st_size > 512 * 1024 * 1024:
                return False
            key = str(path), model.artifact_hash, stat.st_mtime_ns, stat.st_size
            with self._lock:
                if key not in self._cache:
                    with path.open("rb") as stream:
                        valid = (
                            hashlib.file_digest(stream, "sha256").hexdigest() == model.artifact_hash
                        )
                    if len(self._cache) >= 64:
                        self._cache.clear()
                    self._cache[key] = valid
                return self._cache[key]
        except (OSError, ValueError):
            return False

    def describe(self, model: ModelVersion, root: Path) -> dict[str, Any]:
        if not self.ready(model, root):
            return {}
        path = managed_path(root, model.artifact_path)
        with self._lock:
            return dict(self._details.get((str(path), model.artifact_hash), {}))


def model_metadata(
    session: Session,
    settings: Settings,
    artifacts: ArtifactInspector,
    *,
    mode: RunMode | None = None,
) -> ModelMetadata:
    limitations = [
        "Satellite-observed strengthening is a research proxy, not physical fire severity.",
        "Priority categories remain provisional; approval does not establish physical severity.",
    ]
    try:
        version = selected_version(session, settings, mode=mode)
    except ModelRegistryError:
        version = "registry"
    if version in BASELINE_VERSIONS:
        return ModelMetadata(
            scorer_version=version,
            score_kind=ScoreKind.BASELINE,
            feature_version=settings.feature_schema_version,
            model_id=None,
            status="ready",
            artifact_required=False,
            artifact_ready=True,
            scoring_available=True,
            probability_supported=False,
            limitations=limitations,
            supported_modes=[RunMode.LIVE, RunMode.REPLAY],
            calibration_status="not_applicable",
        )
    model = session.scalars(
        select(ModelVersion).where(ModelVersion.version == version)
    ).one_or_none()
    valid = (
        model is not None
        and model.status == ModelStatus.APPROVED
        and model.feature_version == settings.feature_schema_version
    )
    ready = artifacts.ready(model, settings.model_dir) if valid and model is not None else False
    available = bool(
        ready
        and model
        and isinstance(model.training_manifest, dict)
        and model.training_manifest.get("schema_version") == "model-registry-v1"
    )
    scope = (
        model.training_manifest.get("scope", {})
        if valid and model and isinstance(model.training_manifest, dict)
        else {}
    )
    scope = scope if isinstance(scope, dict) else {}
    modes = scope.get("modes", [])
    supported = (
        [
            RunMode(mode)
            for mode in modes
            if isinstance(mode, str) and mode in RunMode._value2member_map_
        ]
        if isinstance(modes, list)
        else []
    )
    try:
        from firewatch.schemas.contracts import BoundingBox

        bbox = BoundingBox.model_validate(scope["bbox"]) if scope.get("bbox") else None
    except ValueError:
        bbox = None
    details = artifacts.describe(model, settings.model_dir) if available and model else {}
    return ModelMetadata(
        scorer_version=version,
        score_kind=ScoreKind.PROBABILITY
        if model and model.probability_supported
        else ScoreKind.MODEL,
        feature_version=model.feature_version if model else None,
        model_id=model.id if model else None,
        status="ready" if available else "unavailable",
        artifact_required=True,
        artifact_ready=ready,
        scoring_available=available,
        probability_supported=bool(valid and model and model.probability_supported),
        limitations=limitations,
        supported_modes=supported,
        approved_region=safe_text(scope.get("region")) or None,
        approved_bbox=bbox,
        **details,
    )


class AnchorReader(Protocol):
    """Optional trusted read-only adapter; Module 5 supplies a real chain implementation."""

    def read_digest(self, anchor: AnchorView) -> str: ...


def verification(
    session: Session,
    alert: Alert,
    run: Run,
    now: datetime,
    settings: Settings,
    reader: AnchorReader | None,
    *,
    verify_artifacts: bool = False,
) -> Verification:
    local_status, computed = verify_payload(alert.canonical_payload, alert.original_hash)
    rows = session.scalars(
        select(Anchor)
        .where(Anchor.alert_id == alert.id)
        .order_by(Anchor.chain_id, Anchor.contract_address)
    ).all()
    views = [
        AnchorView.model_validate(anchor).model_copy(update={"alert_key": alert_key(alert.id)})
        for anchor in rows
    ]
    pending = session.scalar(
        select(AnchorOutbox.id)
        .where(
            AnchorOutbox.alert_id == alert.id,
            AnchorOutbox.status.in_([AnchorStatus.QUEUED, AnchorStatus.SUBMITTED]),
        )
        .limit(1)
    )
    anchor_state = "unanchored"
    if rows:
        anchor_state = "anchor_unavailable"
        confirmed = [view for view in views if view.confirmation_status == AnchorStatus.CONFIRMED]
        if not confirmed and (
            pending
            or any(
                view.confirmation_status in {AnchorStatus.QUEUED, AnchorStatus.SUBMITTED}
                for view in views
            )
        ):
            anchor_state = "anchor_pending"
        elif confirmed and settings.chain_enabled and reader is not None:
            try:
                digests = [
                    reader.read_digest(anchor).removeprefix("0x").lower() for anchor in confirmed
                ]
                if not all(re.fullmatch(r"[0-9a-f]{64}", digest) for digest in digests):
                    raise ValueError("Invalid chain digest")
                anchor_state = (
                    "anchor_match"
                    if all(digest == alert.original_hash for digest in digests)
                    and len(confirmed) == len(views)
                    else "anchor_mismatch"
                )
                if all(digest == alert.original_hash for digest in digests) and (
                    len(confirmed) != len(views)
                ):
                    anchor_state = (
                        "anchor_pending"
                        if pending
                        or any(
                            view.confirmation_status
                            in {AnchorStatus.QUEUED, AnchorStatus.SUBMITTED}
                            for view in views
                        )
                        else "anchor_unavailable"
                    )
            except Exception:
                # Adapter failures/outages do not masquerade as integrity mismatches.
                anchor_state = "anchor_unavailable"
    elif pending:
        anchor_state = "anchor_pending"
    artifact_audit = (
        audit_artifacts(session, alert.canonical_payload, alert.original_hash, settings)
        if verify_artifacts
        else None
    )
    return Verification(
        **context(session, run, now, settings).model_dump(),
        alert_id=alert.id,
        local_status="local_match" if local_status == "local_match" else "local_mismatch",
        original_hash=alert.original_hash,
        computed_hash=computed,
        anchor_status=anchor_state,
        anchors=views,
        **(
            {"artifact_verification": artifact_audit.status, "artifact_audit": artifact_audit}
            if artifact_audit is not None
            else {}
        ),
    )


def evidence_projection(alert: Alert) -> tuple[JsonValue, bool]:
    try:
        payload = json.loads(alert.canonical_payload)
    except (ValueError, UnicodeError):
        return None, True
    projected = public_json(payload)
    return projected, projected != payload
