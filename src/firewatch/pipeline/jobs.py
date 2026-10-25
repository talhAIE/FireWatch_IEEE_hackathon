"""Transactional queue scheduling, lease fencing, and bounded crash recovery."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import func, or_, select, true
from sqlalchemy.orm import Session

from firewatch.config import Settings
from firewatch.db.models import Job, Region, Run
from firewatch.ml.baselines import BASELINE_VERSIONS
from firewatch.ml.registry import ModelRegistryError, resolve_approved, selected_version
from firewatch.repositories.jobs import ENQUEUE_LOCK
from firewatch.schemas.common import JobStatus, JobType, RunMode, RunStatus
from firewatch.sources.firms_normalize import digest
from firewatch.sources.open_meteo import utc


class LeaseLost(RuntimeError):
    """The caller must stop writing after another attempt owns the job."""


@dataclass(frozen=True)
class Lease:
    job_id: UUID
    attempt: int


def _identity(payload: dict[str, Any]) -> tuple[str, str, str]:
    try:
        area = str(UUID(str(payload.get("region_id"))))
        cutoff = utc(datetime.fromisoformat(str(payload.get("as_of")).replace("Z", "+00:00")))
        return area, str(payload.get("mode")), cutoff.isoformat()
    except (ValueError, TypeError):
        return "invalid", "invalid", digest(payload)


def _admission_lock(session: Session, settings: Settings) -> None:
    session.execute(
        select(func.set_config("lock_timeout", str(settings.queue_lock_timeout_ms), True))
    )
    session.execute(select(func.pg_advisory_xact_lock(ENQUEUE_LOCK)))


def schedule(session: Session, settings: Settings, now: datetime) -> int:
    """One current UTC slot per enabled region; no outage catch-up storm."""
    now = utc(now)
    seconds = settings.refresh_interval_minutes * 60
    slot = datetime.fromtimestamp(int(now.timestamp()) // seconds * seconds, UTC)
    _admission_lock(session, settings)
    depth = (
        session.scalar(
            select(func.count())
            .select_from(Job)
            .where(Job.status.in_([JobStatus.QUEUED, JobStatus.RUNNING]))
            .where(Job.type.in_([JobType.REFRESH, JobType.REPLAY]))
        )
        or 0
    )
    created = 0
    for area in session.scalars(select(Region).where(Region.enabled).order_by(Region.id)):
        if depth + created >= settings.max_job_queue_depth:
            break
        if (area.east - area.west) * (area.north - area.south) > settings.max_region_area_degrees2:
            continue
        key = f"schedule:{area.id}:{slot.isoformat()}"
        if session.scalar(select(Job.id).where(Job.idempotency_key == key)) is not None:
            continue
        version = selected_version(session, settings)
        if version not in BASELINE_VERSIONS:
            try:
                resolve_approved(session, settings, version, region=area, mode=RunMode.LIVE)
            except ModelRegistryError:
                # Do not fill the queue with work outside a historical model's review.
                continue
        payload = dict(
            schema_version="jobs-v1",
            region_id=str(area.id),
            mode="live",
            as_of=slot.isoformat(),
            window_start=(slot - timedelta(hours=48)).isoformat(),
            window_end=slot.isoformat(),
            scorer_version=version,
            feature_version=settings.feature_schema_version,
        )
        session.add(
            Job(
                id=uuid4(),
                type=JobType.REFRESH,
                payload=payload,
                payload_hash=digest(payload),
                idempotency_key=key,
                status=JobStatus.QUEUED,
                attempts=0,
                available_at=now,
            )
        )
        created += 1
    session.flush()
    return created


def claim(
    session: Session, settings: Settings, now: datetime, *, job_id: UUID | None = None
) -> Lease | None:
    now = utc(now)
    # Serialize only admission/claim, never provider I/O or stage execution.
    _admission_lock(session, settings)
    due = list(
        session.scalars(
            select(Job)
            .where(Job.type.in_([JobType.REFRESH, JobType.REPLAY]))
            .where(
                or_(
                    (Job.status == JobStatus.QUEUED) & (Job.available_at <= now),
                    (Job.status == JobStatus.RUNNING)
                    & or_(Job.lease_until <= now, Job.lease_until.is_(None)),
                )
            )
            .where(Job.id == job_id if job_id is not None else true())
            .order_by(Job.available_at, Job.id)
            .with_for_update(skip_locked=True)
            .limit(settings.max_job_queue_depth + 20)
        )
    )
    active = list(
        session.scalars(select(Job).where(Job.status == JobStatus.RUNNING, Job.lease_until > now))
    )
    for job in due:
        if job.attempts >= settings.worker_max_attempts:
            completed = session.get(Run, job.result_run_id) if job.result_run_id else None
            if completed is not None and completed.status == RunStatus.COMPLETED:
                job.status = JobStatus.SUCCEEDED
                job.error = None
                job.lease_until = None
                continue
            job.status = JobStatus.FAILED
            job.error = "ATTEMPTS_EXHAUSTED"
            job.lease_until = None
            if job.result_run_id:
                run = session.get(Run, job.result_run_id)
                if run and run.status != RunStatus.COMPLETED:
                    run.status = RunStatus.FAILED
            continue
        identity = _identity(job.payload)
        if any(other.id != job.id and _identity(other.payload) == identity for other in active):
            continue
        job.status = JobStatus.RUNNING
        job.attempts += 1
        job.heartbeat_at = now
        job.lease_until = now + timedelta(seconds=settings.worker_lease_seconds)
        job.error = None
        session.flush()
        return Lease(job.id, job.attempts)
    session.flush()
    return None


def fence(session: Session, lease: Lease, now: datetime) -> Job:
    job = session.scalar(select(Job).where(Job.id == lease.job_id).with_for_update())
    if (
        job is None
        or job.status != JobStatus.RUNNING
        or job.attempts != lease.attempt
        or job.lease_until is None
        or job.lease_until <= utc(now)
    ):
        raise LeaseLost("Job lease expired or ownership changed")
    return job


def heartbeat(session: Session, lease: Lease, settings: Settings, now: datetime) -> None:
    job = fence(session, lease, now)
    job.heartbeat_at = utc(now)
    job.lease_until = utc(now) + timedelta(seconds=settings.worker_lease_seconds)


def finish(
    session: Session,
    lease: Lease,
    settings: Settings,
    now: datetime,
    *,
    error: str | None = None,
    retryable: bool = True,
    retry_after_seconds: float = 0,
) -> None:
    job = fence(session, lease, now)
    job.lease_until = None
    if error is None:
        if job.result_run_id is None:
            raise ValueError("Successful pipeline jobs require a completed run")
        run = session.get(Run, job.result_run_id)
        if run is None or run.status != RunStatus.COMPLETED:
            raise ValueError("Successful pipeline jobs require a completed run")
        job.status = JobStatus.SUCCEEDED
        job.error = None
    else:
        job.error = error  # Only fixed safe codes supplied by the worker.
        retry = retryable and job.attempts < settings.worker_max_attempts
        job.status = JobStatus.QUEUED if retry else JobStatus.FAILED
        delay = max(settings.worker_retry_seconds * 2 ** (job.attempts - 1), retry_after_seconds)
        job.available_at = utc(now) + timedelta(seconds=min(delay, 86400))
        if not retry and job.result_run_id:
            run = session.get(Run, job.result_run_id)
            if run and run.status != RunStatus.COMPLETED:
                run.status = RunStatus.FAILED
