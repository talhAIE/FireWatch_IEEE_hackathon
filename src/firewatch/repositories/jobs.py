"""Short atomic API enqueue transactions; worker execution remains Module 3."""

import hashlib
import json
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import yaml
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from firewatch.api.errors import APIError
from firewatch.config import Settings
from firewatch.db.models import Alert, Anchor, AnchorOutbox, Job, Region, SourceAsset
from firewatch.ml.baselines import BASELINE_VERSIONS
from firewatch.ml.registry import ModelRegistryError, resolve_approved, selected_version
from firewatch.repositories.reads import require_run
from firewatch.schemas.common import JobStatus, JobType, RunMode
from firewatch.schemas.operator import RefreshRequest, ReplayConfig, ReplayRequest

# Every API producer uses this transaction-scoped PostgreSQL lock, including across processes.
ENQUEUE_LOCK = 0x46574A4F4253


def enqueue(
    session: Session,
    settings: Settings,
    kind: JobType,
    key: str,
    identity: dict[str, Any],
    now: datetime,
    build_payload: Callable[[], dict[str, Any]],
) -> tuple[Job, bool]:
    digest = hashlib.sha256(
        json.dumps(
            {"operation": kind.value, "request": identity},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode()
    ).hexdigest()
    with session.begin():
        session.execute(
            select(func.set_config("lock_timeout", str(settings.queue_lock_timeout_ms), True))
        )
        session.execute(select(func.pg_advisory_xact_lock(ENQUEUE_LOCK)))
        existing = session.scalar(select(Job).where(Job.idempotency_key == "operator:" + key))
        if existing is not None:
            if existing.type != kind or existing.payload_hash != digest:
                raise APIError(
                    409, "IDEMPOTENCY_CONFLICT", "This key belongs to a different request."
                )
            # Retrying admitted work must succeed even after capacity/configuration changes.
            session.expunge(existing)
            return existing, True
        depth = (
            session.scalar(
                select(func.count())
                .select_from(Job)
                .where(Job.status.in_([JobStatus.QUEUED, JobStatus.RUNNING]))
                .where(
                    Job.type == JobType.ANCHOR
                    if kind == JobType.ANCHOR
                    else Job.type.in_([JobType.REFRESH, JobType.REPLAY])
                )
            )
            or 0
        )
        if depth >= settings.max_job_queue_depth:
            raise APIError(503, "QUEUE_FULL", "The job queue is full; retry later.", True)
        payload = build_payload()
        job = Job(
            id=uuid4(),
            type=kind,
            payload=payload,
            idempotency_key="operator:" + key,
            payload_hash=digest,
            status=JobStatus.QUEUED,
            attempts=0,
            available_at=now,
        )
        session.add(job)
        session.flush()
        session.expunge(job)
    # The transaction has committed before an acceptance response can be constructed.
    return job, False


def allowed_region(
    session: Session,
    settings: Settings,
    *,
    identifier: UUID | None = None,
    name: str | None = None,
    allow_disabled: bool = False,
) -> Region:
    condition = Region.id == identifier if identifier is not None else Region.name == name
    record = session.scalar(select(Region).where(condition).with_for_update(read=True))
    if record is None:
        raise APIError(404, "REGION_NOT_FOUND", "The requested region was not found.")
    if not record.enabled and not allow_disabled:
        raise APIError(422, "REGION_DISABLED", "The requested region is not enabled.")
    if (record.east - record.west) * (
        record.north - record.south
    ) > settings.max_region_area_degrees2:
        raise APIError(422, "REGION_TOO_LARGE", "The region exceeds the operator request bound.")
    return record


def refresh_payload(
    session: Session, settings: Settings, body: RefreshRequest, now: datetime
) -> dict[str, Any]:
    area = allowed_region(session, settings, identifier=body.region_id)
    if (
        body.window_end > now
        or now - body.window_start > timedelta(hours=settings.max_refresh_age_hours)
        or body.window_end - body.window_start > timedelta(hours=settings.max_refresh_window_hours)
    ):
        raise APIError(
            422, "INVALID_REFRESH_WINDOW", "The refresh window exceeds configured bounds."
        )
    try:
        version = selected_version(session, settings, mode=RunMode.LIVE)
        if version not in BASELINE_VERSIONS:
            resolve_approved(session, settings, version, region=area, mode=RunMode.LIVE)
    except ModelRegistryError:
        raise APIError(
            503, "MODEL_UNAVAILABLE", "Configured model is not approved for this request."
        ) from None
    return {
        "schema_version": "jobs-v1",
        **body.model_dump(mode="json"),
        "region_id": str(area.id),
        "mode": "live",
        "as_of": body.window_end.isoformat(),
        "scorer_version": version,
        "feature_version": settings.feature_schema_version,
    }


def replay_payload(
    session: Session, settings: Settings, body: ReplayRequest, now: datetime
) -> dict[str, Any]:
    try:
        with settings.replays_config.open("rb") as stream:
            raw = stream.read(256 * 1024 + 1)
        if len(raw) > 256 * 1024:
            raise ValueError("Oversized configuration")
        configuration = ReplayConfig.model_validate(yaml.safe_load(raw))
    except (OSError, ValueError, RecursionError, yaml.YAMLError):
        raise APIError(
            503, "REPLAY_CONFIG_UNAVAILABLE", "Replay configuration is unavailable.", True
        ) from None
    scenario = next((item for item in configuration.scenarios if item.id == body.scenario_id), None)
    if scenario is None:
        raise APIError(
            404, "SCENARIO_NOT_FOUND", "The requested replay scenario is not allowlisted."
        )
    if scenario.bundle_sha256:
        from firewatch.pipeline.replay_bundle import load_bundle

        try:
            bundle = load_bundle(settings.replay_bundle_dir / scenario.id, scenario.bundle_sha256)
            if bundle.scenario != scenario.model_copy(update={"bundle_sha256": None}):
                raise ValueError("Catalog and replay bundle disagree")
        except (OSError, ValueError):
            raise APIError(
                503,
                "REPLAY_BUNDLE_UNAVAILABLE",
                "Required replay bundle is missing or failed verification.",
                True,
            ) from None
    if scenario.window_end > now or scenario.window_end - scenario.window_start > timedelta(
        days=settings.max_replay_window_days
    ):
        raise APIError(422, "INVALID_REPLAY_WINDOW", "The configured replay window exceeds bounds.")
    area = allowed_region(
        session,
        settings,
        name=scenario.region_name,
        allow_disabled=scenario.bundle_sha256 is not None,
    )
    available = set(
        session.scalars(select(SourceAsset.id).where(SourceAsset.id.in_(scenario.asset_ids)))
    )
    if available != set(scenario.asset_ids):
        raise APIError(
            503, "REPLAY_ASSETS_UNAVAILABLE", "Required replay assets are not imported.", True
        )
    return {
        "schema_version": "jobs-v1",
        **scenario.model_dump(mode="json", exclude={"id", "name", "region_name", "bundle_sha256"}),
        **({"bundle_sha256": scenario.bundle_sha256} if scenario.bundle_sha256 else {}),
        "scenario_id": scenario.id,
        "scenario_name": scenario.name,
        "region_id": str(area.id),
        "mode": "replay",
        "as_of": scenario.window_end.isoformat(),
        "asset_ids": sorted(str(identifier) for identifier in scenario.asset_ids),
    }


def anchor_payload(
    session: Session, settings: Settings, alert_id: UUID, now: datetime
) -> dict[str, Any]:
    if not settings.chain_enabled:
        raise APIError(503, "CHAIN_DISABLED", "Optional anchoring is disabled.")
    alert = session.get(Alert, alert_id)
    if alert is None:
        raise APIError(404, "ALERT_NOT_FOUND", "The requested alert was not found.")
    require_run(session, alert.run_id)
    if hashlib.sha256(alert.canonical_payload).hexdigest() != alert.original_hash:
        raise APIError(409, "EVIDENCE_MISMATCH", "Saved evidence failed its local integrity check.")
    address = settings.alert_registry_address.lower()
    network = f"eip155:{settings.chain_id}:{address}"
    outbox = session.scalar(
        select(AnchorOutbox.id)
        .where(
            AnchorOutbox.alert_id == alert_id,
            or_(AnchorOutbox.network == network, ~AnchorOutbox.network.like("eip155:%")),
        )
        .limit(1)
    )
    anchored = session.scalar(
        select(Anchor.id)
        .where(
            Anchor.alert_id == alert_id,
            Anchor.chain_id == settings.chain_id,
            func.lower(Anchor.contract_address) == address,
        )
        .limit(1)
    )
    if outbox is not None or anchored is not None:
        raise APIError(
            409, "ANCHOR_ALREADY_SCHEDULED", "Anchoring already exists for this alert and network."
        )
    identifier = uuid4()
    session.add(AnchorOutbox(id=identifier, alert_id=alert_id, network=network, available_at=now))
    return {
        "schema_version": "jobs-v1",
        "alert_id": str(alert_id),
        "outbox_id": str(identifier),
        "network": network,
        "chain_id": settings.chain_id,
        "contract_address": address,
        "original_hash": alert.original_hash,
    }
