"""Read-only operational observations; configuration is never presented as live proof."""

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from firewatch.config import Settings
from firewatch.db.models import Anchor, Job, Region, Run
from firewatch.runtime import queue_progress
from firewatch.schemas.api import ChainReadiness, LiveReadiness, ModelMetadata, WorkerObservation
from firewatch.schemas.common import AnchorStatus, JobStatus, JobType, RunMode, RunStatus


def operational_status(
    session: Session,
    settings: Settings,
    model: ModelMetadata,
    now: datetime,
    latest_live_observation: datetime | None,
) -> dict[str, Any]:
    kinds = [JobType.REFRESH, JobType.REPLAY]
    pending = (
        session.scalar(
            select(func.count())
            .select_from(Job)
            .where(Job.type.in_(kinds), Job.status.in_([JobStatus.QUEUED, JobStatus.RUNNING]))
        )
        or 0
    )
    heartbeat = session.scalar(select(func.max(Job.heartbeat_at)).where(Job.type.in_(kinds)))
    recent = (
        heartbeat is not None
        and now - timedelta(seconds=settings.worker_health_max_lag_seconds) <= heartbeat <= now
    )
    worker = WorkerObservation(
        state="stalled"
        if not queue_progress(session, settings, now)
        else "recent_activity"
        if recent
        else "unverified",
        pending_jobs=pending,
        latest_heartbeat_at=heartbeat,
    )
    enabled = list(session.scalars(select(Region).where(Region.enabled)))
    eligible = []
    for area in enabled:
        box = model.approved_bbox
        region_valid = model.model_id is None or (
            box is not None
            and area.name == model.approved_region
            and box.west <= area.west < area.east <= box.east
            and box.south <= area.south < area.north <= box.north
        )
        within_request_limits = (area.east - area.west) * (
            area.north - area.south
        ) <= settings.max_region_area_degrees2
        if (
            region_valid
            and within_request_limits
            and RunMode.LIVE in model.supported_modes
            and model.scoring_available
        ):
            eligible.append(area.id)
    blockers = []
    if not settings.firms_map_key:
        blockers.append("FIRMS_KEY_NOT_CONFIGURED")
    if not enabled:
        blockers.append("NO_ENABLED_REGIONS")
    if not model.scoring_available:
        blockers.append("SCORER_UNAVAILABLE")
    if RunMode.LIVE not in model.supported_modes:
        blockers.append("SCORER_NOT_APPROVED_FOR_LIVE")
    if enabled and not eligible:
        blockers.append("NO_REGION_WITHIN_LIVE_SCORER_SCOPE")
    live = LiveReadiness(
        scorer=model,
        configuration_ready=not blockers,
        eligible_region_ids=eligible,
        recent_live_data_available=latest_live_observation is not None
        and 0
        <= (now - latest_live_observation).total_seconds()
        <= settings.detection_max_age_hours * 3600,
        blockers=blockers
        + (
            ["WORKER_STALLED"]
            if worker.state == "stalled"
            else ["WORKER_NOT_OBSERVED"]
            if worker.state == "unverified"
            else []
        ),
    )
    modes = set(
        session.scalars(select(Run.mode).where(Run.status == RunStatus.COMPLETED).distinct())
    )
    chain_blockers = []
    if not settings.chain_enabled:
        chain_blockers.append("CHAIN_DISABLED")
    elif not settings.chain_private_key:
        chain_blockers.append("CHAIN_SIGNER_NOT_CONFIGURED")
    if settings.chain_network_kind == "local":
        chain_blockers.append("LOCAL_CHAIN_IS_NOT_INDEPENDENT_NOTARIZATION")
    chain = ChainReadiness(
        state="disabled"
        if not settings.chain_enabled
        else "signing_configured"
        if settings.chain_private_key
        else "read_only",
        network_kind=settings.chain_network_kind,
        confirmed_anchor_records=session.scalar(
            select(func.count())
            .select_from(Anchor)
            .where(Anchor.confirmation_status == AnchorStatus.CONFIRMED)
        )
        or 0,
        blockers=chain_blockers,
    )
    return {
        "worker": worker,
        "live_readiness": live,
        "chain_readiness": chain,
        "public_reads_enabled": settings.public_reads_enabled,
        "cors_origins": settings.cors_origins,
        "data_mode": "mixed"
        if len(modes) == 2
        else "historical_only"
        if RunMode.REPLAY in modes
        else "live"
        if RunMode.LIVE in modes
        else "empty",
    }
