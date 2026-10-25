"""Dedicated worker process; scheduling is never attached to an ASGI lifecycle."""

import argparse
import logging
import signal
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from firewatch.config import Settings, get_settings
from firewatch.db.models import Run
from firewatch.db.session import Database
from firewatch.evidence.storage import StorageError, StorageIntegrityError
from firewatch.ml.registry import ModelRegistryError, ModelScoringError
from firewatch.pipeline.jobs import Lease, LeaseLost, claim, fence, finish, heartbeat, schedule
from firewatch.pipeline.runner import execute_run
from firewatch.schemas.common import JobType, RunStatus, Stage
from firewatch.sources.firms import FirmsClient, FirmsError
from firewatch.sources.open_meteo import OpenMeteoClient
from firewatch.sources.weather_budget import WeatherBudget

log = logging.getLogger("firewatch.worker")


@contextmanager
def keep_alive(
    engine: Engine, settings: Settings, lease: Lease, now: Callable[[], datetime]
) -> Iterator[threading.Event]:
    stop, lost = threading.Event(), threading.Event()

    def pulse() -> None:
        while not stop.wait(settings.worker_lease_seconds / 3):
            try:
                with Session(engine) as session, session.begin():
                    heartbeat(session, lease, settings, now())
            except Exception:
                lost.set()
                return  # Never log database/provider exceptions containing credentials.

    thread = threading.Thread(target=pulse, name="firewatch-heartbeat", daemon=True)
    thread.start()
    try:
        yield lost
    finally:
        stop.set()
        thread.join(timeout=settings.db_statement_timeout_ms / 1000 + 1)


def process_one(
    engine: Engine,
    settings: Settings,
    *,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    executor: Callable[..., object] = execute_run,
    job_id: UUID | None = None,
) -> bool:
    with Session(engine) as session, session.begin():
        lease = claim(session, settings, now(), job_id=job_id)
    if lease is None:
        return False
    error, retryable, retry_after = None, True, 0.0
    firms, weather = None, None
    try:
        with keep_alive(engine, settings, lease, now) as lost:
            with Session(engine) as session, session.begin():
                job = fence(session, lease, now())
                live = job.type == JobType.REFRESH
            if live:
                if settings.firms_map_key:
                    firms = FirmsClient(
                        settings.firms_map_key, output_dir=settings.raw_data_dir / "firms", now=now
                    )
                weather = OpenMeteoClient(
                    settings.raw_data_dir / "weather",
                    WeatherBudget(settings.raw_data_dir / "weather-budget"),
                    now=now,
                )
            executor(engine, settings, lease, now, firms=firms, weather=weather)
            if lost.is_set():
                raise LeaseLost("Heartbeat failed")
    except LeaseLost:
        log.warning("job=%s attempt=%s lease_lost", lease.job_id, lease.attempt)
        return True  # A stale attempt cannot change the new owner's status.
    except FirmsError as exc:
        error, retryable = "SATELLITE_SOURCE_FAILED", exc.retryable
        retry_after = exc.retry_after_seconds or 0
    except ModelScoringError:
        error, retryable = "MODEL_SCORING_WITHHELD", False
    except ModelRegistryError:
        error, retryable = "MODEL_APPROVAL_UNAVAILABLE", False
    except StorageIntegrityError:
        error, retryable = "EVIDENCE_STORAGE_MISMATCH", False
    except StorageError:
        error, retryable = "EVIDENCE_STORAGE_UNAVAILABLE", True
    except (ValueError, OSError):
        error, retryable = "INVALID_INPUT_OR_CONFIGURATION", False
    except Exception:
        error = "PIPELINE_FAILED"
    finally:
        if firms is not None:
            firms.close()
        if weather is not None:
            weather.close()
    try:
        with Session(engine) as session, session.begin():
            job = fence(session, lease, now())
            if error and job.result_run_id:
                run = session.get(Run, job.result_run_id)
                if run is not None and run.status != RunStatus.COMPLETED:
                    pending = next(
                        (s for s in Stage if run.stage_status.get(s.value) != "succeeded"), None
                    )
                    if pending:
                        run.stage_status = dict(run.stage_status) | {pending.value: "failed"}
            finish(
                session,
                lease,
                settings,
                now(),
                error=error,
                retryable=retryable,
                retry_after_seconds=retry_after,
            )
    except LeaseLost:
        log.warning("job=%s attempt=%s lease_lost", lease.job_id, lease.attempt)
        return True
    log.info("job=%s attempt=%s result=%s", lease.job_id, lease.attempt, error or "succeeded")
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FireWatch dedicated durable job worker")
    parser.add_argument(
        "--once", action="store_true", help="Schedule once and process at most one job"
    )
    parser.add_argument("--no-schedule", action="store_true", help="Only consume admitted jobs")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = get_settings()
    database = Database(settings)
    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    try:
        while not stop.is_set():
            try:
                if not args.no_schedule:
                    with database.transaction() as session:
                        schedule(session, settings, datetime.now(UTC))
                worked = process_one(database.engine, settings)
            except ModelRegistryError:
                log.error("Configured model registry is unavailable; no baseline substitution")
                worked = False
            except SQLAlchemyError:
                log.error("Worker database operation failed; retrying after poll interval")
                worked = False
            if args.once:
                return 0
            if not worked:
                stop.wait(settings.worker_poll_seconds)
    finally:
        database.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
