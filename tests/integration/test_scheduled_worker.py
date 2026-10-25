"""A scheduled batch is bounded, excludes replays and reports incomplete jobs as failures."""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from conftest import migrate
from pydantic import SecretStr
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_schema import region
from test_worker import CLOCK, queued, settings

from firewatch import scheduled_worker
from firewatch.db.models import Job, LiveRunHead
from firewatch.evidence.storage import StorageError, SupabaseStore
from firewatch.pipeline.runner import execute_run
from firewatch.schemas.common import JobStatus
from firewatch.worker import process_one

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("succeeds", [False, True])
def test_batch_only_consumes_bounded_live_jobs_and_returns_truthful_status(
    empty_engine, tmp_path, monkeypatch, succeeds
):
    migrate(empty_engine)
    config = settings(tmp_path)
    config.firms_map_key = SecretStr("synthetic-firms-key")
    config.supabase_storage_enabled = True
    with Session(empty_engine) as session, session.begin():
        area = region(session)
        queued(session, area.id, mode="replay")
        live_ids = [queued(session, area.id, mode="live") for _ in range(4)]

    @contextmanager
    def transaction():
        with Session(empty_engine) as session, session.begin():
            yield session

    database = SimpleNamespace(
        engine=empty_engine,
        session=lambda: Session(empty_engine),
        transaction=transaction,
        close=lambda: None,
    )
    monkeypatch.setattr(scheduled_worker, "get_settings", lambda: config)
    monkeypatch.setattr(scheduled_worker, "Database", lambda _: database)
    monkeypatch.setattr(scheduled_worker, "wait_for_database", lambda *a, **kw: True)
    monkeypatch.setattr(scheduled_worker, "schedule", lambda *a: 0)
    processed = []

    def process(engine, settings, *, job_id):
        assert job_id in live_ids
        processed.append(job_id)
        with Session(engine) as session, session.begin():
            session.get(Job, job_id).status = JobStatus.SUCCEEDED if succeeds else JobStatus.QUEUED
        return True

    monkeypatch.setattr(scheduled_worker, "process_one", process)
    assert scheduled_worker.main(["--max-jobs", "2"]) == (0 if succeeds else 1)
    assert len(processed) == 2 and len(set(processed)) == 2


def test_storage_outage_withholds_publication_and_keeps_job_retryable(
    empty_engine, tmp_path, monkeypatch
):
    from refresh_factory import FakeRefreshClient

    migrate(empty_engine)
    config = settings(tmp_path)
    config.supabase_storage_enabled = True
    config.supabase_url = "https://synthetic-project.supabase.co"
    config.supabase_secret_key = SecretStr("synthetic-storage-unit-key")
    with Session(empty_engine) as session, session.begin():
        area = region(session)
        area.enabled = True
        identifier = queued(session, area.id, mode="live")

    def unavailable(*args):
        raise StorageError("Private evidence upload failed")

    monkeypatch.setattr(SupabaseStore, "put", unavailable)
    provider = FakeRefreshClient(tmp_path / "source", clock=lambda: CLOCK)

    def executor(engine, settings, lease, now, **kwargs):
        return execute_run(engine, settings, lease, now, firms=provider)

    assert process_one(empty_engine, config, now=lambda: CLOCK, executor=executor)
    with Session(empty_engine) as session:
        job = session.get(Job, identifier)
        assert job.status == JobStatus.QUEUED
        assert job.error == "EVIDENCE_STORAGE_UNAVAILABLE"
        assert session.scalar(select(LiveRunHead)) is None
