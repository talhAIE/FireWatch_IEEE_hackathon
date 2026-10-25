"""One bounded live refresh batch for scheduled CI; no migrations, seeding or endless loop."""

import argparse
import logging
import threading
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from firewatch.config import get_settings
from firewatch.db.models import Job
from firewatch.db.session import Database
from firewatch.logging import install_redaction
from firewatch.pipeline.jobs import schedule
from firewatch.runtime import wait_for_database
from firewatch.schemas.common import JobStatus, JobType
from firewatch.worker import process_one


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-jobs", type=int, choices=range(1, 6), default=3)
    args = parser.parse_args(argv)
    settings = get_settings()
    if not settings.firms_map_key or not settings.supabase_storage_enabled:
        print("Scheduled inference requires FIRMS and private evidence storage configuration.")
        return 2
    logging.basicConfig(level=logging.INFO)
    redact = install_redaction(settings)
    database = Database(settings)
    try:
        if not wait_for_database(settings, schema=True, stop=threading.Event()):
            return 1
        with database.transaction() as session:
            created = schedule(session, settings, datetime.now(UTC))
        with database.session() as session:
            identifiers = list(
                session.scalars(
                    select(Job.id)
                    .where(
                        Job.type == JobType.REFRESH,
                        Job.status.in_([JobStatus.QUEUED, JobStatus.RUNNING]),
                        Job.available_at <= datetime.now(UTC),
                    )
                    .order_by(Job.available_at, Job.id)
                    .limit(args.max_jobs)
                )
            )
        completed, incomplete = 0, 0
        for identifier in identifiers:
            if process_one(database.engine, settings, job_id=identifier):
                with Session(database.engine) as session:
                    job = session.get(Job, identifier)
                    if job is not None and job.status == JobStatus.SUCCEEDED:
                        completed += 1
                    else:
                        incomplete += 1
        print(f"Live batch: scheduled={created} succeeded={completed} incomplete={incomplete}")
        return int(incomplete > 0)
    finally:
        database.close()
        redact()


if __name__ == "__main__":
    raise SystemExit(main())
