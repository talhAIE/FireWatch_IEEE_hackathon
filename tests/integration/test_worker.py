"""Durable worker acceptance against migrated disposable PostgreSQL schemas."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from uuid import uuid4

import pytest
from conftest import migrate
from firms_factory import NOW, ROW
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session
from test_schema import T, region
from test_snapshot_persistence import ingest, setup_sources

from firewatch.config import Settings
from firewatch.db.models import Alert, AnchorOutbox, Job, LiveRunHead, Run, Score
from firewatch.pipeline.jobs import LeaseLost, claim, fence, finish, heartbeat, schedule
from firewatch.pipeline.runner import execute_run
from firewatch.schemas.common import JobStatus, JobType, RunMode, Stage
from firewatch.sources.firms_normalize import digest
from firewatch.worker import process_one

pytestmark = pytest.mark.integration
CLOCK = NOW + timedelta(days=2)


def settings(tmp_path, **overrides):
    return Settings(
        database_url="postgresql+psycopg://test:password@localhost:1/firewatch_test",
        admin_api_token="test-only-" + "x" * 40,
        raw_data_dir=tmp_path,
        worker_retry_seconds=1,
        _env_file=None,
        **overrides,
    )


def queued(session, area_id, *, assets=None, mode="replay", cutoff=T, key=None):
    payload = dict(
        schema_version="jobs-v1",
        region_id=str(area_id),
        mode=mode,
        as_of=cutoff.isoformat(),
        window_start=(cutoff - timedelta(hours=48)).isoformat(),
        window_end=cutoff.isoformat(),
        scorer_version="baseline-v1",
        feature_version="features-v1",
    )
    if assets:
        payload["asset_ids"] = [str(i) for i in assets]
    job = Job(
        id=uuid4(),
        type=JobType.REPLAY if mode == "replay" else JobType.REFRESH,
        payload=payload,
        payload_hash=digest(payload),
        idempotency_key=key or str(uuid4()),
        attempts=0,
        status=JobStatus.QUEUED,
        available_at=CLOCK,
    )
    session.add(job)
    session.flush()
    return job.id


@pytest.fixture
def work(empty_engine, tmp_path):
    migrate(empty_engine)
    with Session(empty_engine) as session, session.begin():
        area, _, policy = setup_sources(session, tmp_path)
        area.enabled = True
        # Use real normalized synthetic source revisions with critical FRP.
        ingest(
            session,
            tmp_path,
            area,
            [ROW | {"acq_time": "1100", "frp": "150"}],
            retrieved=NOW + timedelta(hours=1),
        )
        from firewatch.db.models import IngestionBatch

        assets = list(
            session.scalars(
                select(IngestionBatch.asset_id).where(IngestionBatch.region_id == area.id)
            )
        )
        identifier = queued(session, area.id, assets=assets)
        area_id = area.id
    return empty_engine, settings(tmp_path), identifier, area_id


def take(engine, config, clock=CLOCK):
    with Session(engine) as session, session.begin():
        return claim(session, config, clock)


def test_scheduler_slots_concurrency_and_disabled_regions(empty_engine, tmp_path):
    migrate(empty_engine)
    with Session(empty_engine) as session, session.begin():
        area = region(session)
        area.enabled = True
        region(session)  # Disabled region is not scheduled.
    config = settings(tmp_path)

    def tick(_):
        with Session(empty_engine) as session, session.begin():
            return schedule(session, config, CLOCK)

    with ThreadPoolExecutor(max_workers=3) as pool:
        assert sum(pool.map(tick, range(3))) == 1
    with Session(empty_engine) as session, session.begin():
        assert schedule(session, config, CLOCK + timedelta(minutes=15)) == 1
    with Session(empty_engine) as session:
        jobs = list(session.scalars(select(Job)))
        assert len(jobs) == 2
        assert all(j.payload["mode"] == "live" for j in jobs)


def test_claim_is_exclusive_and_same_cutoff_does_not_overlap(work):
    engine, config, identifier, area_id = work
    with Session(engine) as session, session.begin():
        original = session.get(Job, identifier)
        queued(session, area_id, assets=original.payload["asset_ids"])
    with ThreadPoolExecutor(max_workers=2) as pool:
        leases = list(pool.map(lambda _: take(engine, config), range(2)))
    assert sum(x is not None for x in leases) == 1


def test_heartbeat_expiry_and_stale_owner_fencing(work):
    engine, config, _, _ = work
    old = take(engine, config)
    with Session(engine) as session, session.begin():
        heartbeat(session, old, config, CLOCK + timedelta(seconds=60))
    assert take(engine, config, CLOCK + timedelta(seconds=121)) is None
    newer = take(engine, config, CLOCK + timedelta(seconds=181))
    assert newer.attempt == 2
    with Session(engine) as session, session.begin(), pytest.raises(LeaseLost):
        fence(session, old, CLOCK + timedelta(seconds=181))


def test_retry_backoff_bounded_attempts_and_safe_errors(work):
    engine, config, identifier, _ = work
    config.worker_max_attempts = 2
    lease = take(engine, config)
    with Session(engine) as session, session.begin():
        finish(session, lease, config, CLOCK, error="PIPELINE_FAILED")
    assert take(engine, config) is None
    lease = take(engine, config, CLOCK + timedelta(seconds=1))
    with Session(engine) as session, session.begin():
        finish(session, lease, config, CLOCK + timedelta(seconds=1), error="PIPELINE_FAILED")
    assert take(engine, config, CLOCK + timedelta(days=1)) is None
    with Session(engine) as session:
        assert session.get(Job, identifier).status == JobStatus.FAILED


def test_expired_last_attempt_is_terminal(work):
    engine, config, identifier, _ = work
    config.worker_max_attempts = 1
    take(engine, config)
    assert take(engine, config, CLOCK + timedelta(seconds=121)) is None
    with Session(engine) as session:
        assert session.get(Job, identifier).error == "ATTEMPTS_EXHAUSTED"


def test_offline_pipeline_scores_evidence_and_never_publishes_live(work, monkeypatch):
    engine, config, identifier, _ = work
    import httpx

    monkeypatch.setattr(httpx.Client, "send", lambda *a, **k: pytest.fail("Replay used network"))
    assert process_one(engine, config, now=lambda: CLOCK)
    with Session(engine) as session:
        job = session.get(Job, identifier)
        assert job.status == JobStatus.SUCCEEDED
        run = session.get(Run, job.result_run_id)
        assert run.status == "completed" and run.mode == RunMode.REPLAY
        assert run.completeness == "partial"
        assert all(run.stage_status[s.value] == "succeeded" for s in Stage)
        assert session.scalar(select(func.count()).select_from(LiveRunHead)) == 0
        score = session.scalar(select(Score).where(Score.run_id == run.id))
        assert score.priority_score == 150 and score.escalation_probability is None
        alert = session.scalar(select(Alert).where(Alert.run_id == run.id))
        assert alert is not None
        import hashlib

        assert hashlib.sha256(alert.canonical_payload).hexdigest() == alert.original_hash
        assert session.scalar(select(func.count()).select_from(AnchorOutbox)) == 0


@pytest.mark.parametrize("stage", list(Stage))
def test_crash_after_each_committed_stage_resumes_without_duplicates(work, stage):
    engine, config, identifier, _ = work
    first = take(engine, config)

    def crash(current):
        if current == stage:
            raise SystemExit("simulated worker termination")

    with pytest.raises(SystemExit):
        execute_run(engine, config, first, lambda: CLOCK, after_stage=crash)
    with Session(engine) as session:
        run_id = session.get(Job, identifier).result_run_id
        assert session.get(Run, run_id).status != "completed"
    later = CLOCK + timedelta(seconds=config.worker_lease_seconds + 1)
    lease = take(engine, config, later)
    result = execute_run(engine, config, lease, lambda: later)
    with Session(engine) as session, session.begin():
        finish(session, lease, config, later)
    assert result == run_id
    with Session(engine) as session:
        assert session.scalar(select(func.count()).select_from(Score)) == 1
        assert session.scalar(select(func.count()).select_from(Alert)) == 1
        assert session.get(Run, run_id).status == "completed"


def test_crash_after_publish_before_job_acknowledgement(work):
    engine, config, identifier, _ = work
    lease = take(engine, config)
    first = execute_run(engine, config, lease, lambda: CLOCK)
    later = CLOCK + timedelta(seconds=121)
    lease = take(engine, config, later)
    assert execute_run(engine, config, lease, lambda: later) == first
    with Session(engine) as session, session.begin():
        finish(session, lease, config, later)
    with Session(engine) as session:
        assert session.get(Job, identifier).status == JobStatus.SUCCEEDED
        assert session.scalar(select(func.count()).select_from(Alert)) == 1


def test_inference_has_no_database_row_locks(work, monkeypatch):
    engine, config, identifier, _ = work
    import firewatch.pipeline.runner as runner

    original = runner.score_features

    def check(*args, **kwargs):
        with Session(engine) as session, session.begin():
            # This would raise immediately if the pipeline kept the job lock during inference.
            session.scalar(select(Job).where(Job.id == identifier).with_for_update(nowait=True))
        return original(*args, **kwargs)

    monkeypatch.setattr(runner, "score_features", check)
    assert process_one(engine, config, now=lambda: CLOCK)
    with Session(engine) as session:
        assert session.get(Job, identifier).status == JobStatus.SUCCEEDED


def test_inference_failure_does_not_publish_and_retry_uses_checkpoint(work, monkeypatch):
    engine, config, identifier, _ = work
    import firewatch.pipeline.runner as runner

    original = runner.score_features
    monkeypatch.setattr(
        runner,
        "score_features",
        lambda *a, **k: (_ for _ in ()).throw(
            RuntimeError("secret database_url=never-return-this")
        ),
    )
    assert process_one(engine, config, now=lambda: CLOCK)
    with Session(engine) as session:
        job = session.get(Job, identifier)
        assert job.error == "PIPELINE_FAILED" and job.status == "queued"
        assert session.get(Run, job.result_run_id).stage_status["scoring"] == "failed"
        assert session.scalar(select(func.count()).select_from(LiveRunHead)) == 0
    monkeypatch.setattr(runner, "score_features", original)
    assert process_one(engine, config, now=lambda: CLOCK + timedelta(seconds=1))
    with Session(engine) as session:
        assert session.get(Job, identifier).status == JobStatus.SUCCEEDED


def test_empty_pipeline_is_successful(empty_engine, tmp_path):
    migrate(empty_engine)
    with Session(empty_engine) as session, session.begin():
        area, _, policy = setup_sources(session, tmp_path, empty=True)
        area.enabled = True
        from firewatch.db.models import IngestionBatch

        assets = list(
            session.scalars(
                select(IngestionBatch.asset_id).where(IngestionBatch.id.in_(policy.batch_ids))
            )
        )
        identifier = queued(session, area.id, assets=assets)
    assert process_one(empty_engine, settings(tmp_path), now=lambda: CLOCK)
    with Session(empty_engine) as session:
        job = session.get(Job, identifier)
        assert job.status == JobStatus.SUCCEEDED
        assert session.get(Run, job.result_run_id).completeness == "complete"
        assert session.scalar(select(func.count()).select_from(Score)) == 0


def test_anchor_only_queue_is_left_for_anchor_executor(work):
    engine, config, identifier, _ = work
    with Session(engine) as session, session.begin():
        session.execute(update(Job).where(Job.id == identifier).values(type=JobType.ANCHOR))
    assert not process_one(engine, config, now=lambda: CLOCK)
    with Session(engine) as session:
        job = session.get(Job, identifier)
        assert job.status == JobStatus.QUEUED and job.attempts == 0 and job.error is None


@pytest.mark.parametrize("degraded", [False, True])
@pytest.mark.parametrize("separate_live_scorer", [False, True])
@pytest.mark.parametrize("remote_storage", [False, True])
def test_live_pipeline_reuses_source_chunks_and_publishes_only_after_evidence(
    empty_engine, tmp_path, degraded, separate_live_scorer, remote_storage, monkeypatch
):
    from refresh_factory import FakeRefreshClient

    from firewatch.schemas.common import Product
    from firewatch.sources.firms import FirmsError
    from firewatch.sources.weather_budget import WeatherError

    migrate(empty_engine)
    with Session(empty_engine) as session, session.begin():
        area = region(session)
        area.enabled = True
        identifier = queued(session, area.id, mode="live")
        area_id = area.id
    config = settings(
        tmp_path,
        active_scorer="registry" if separate_live_scorer else "baseline-v1",
        live_scorer="baseline-v1" if separate_live_scorer else None,
    )
    objects = {}
    if remote_storage:
        from pydantic import SecretStr

        from firewatch.evidence.storage import SupabaseStore

        config.supabase_storage_enabled = True
        config.supabase_url = "https://synthetic-project.supabase.co"
        config.supabase_secret_key = SecretStr("synthetic-storage-unit-key")

        def put(self, checksum, content):
            with Session(empty_engine) as session:
                assert session.scalar(select(func.count()).select_from(LiveRunHead)) == 0
            objects[checksum] = content

        monkeypatch.setattr(SupabaseStore, "put", put)
        monkeypatch.setattr(
            SupabaseStore, "get", lambda self, checksum, **kw: objects.get(checksum)
        )
    client = FakeRefreshClient(tmp_path / "live", clock=lambda: CLOCK)
    client.frp = "150"
    client.failures[(Product.NOAA21_NRT, (T - timedelta(hours=48)).date())] = [
        FirmsError("NETWORK", retryable=True)
    ]
    first = take(empty_engine, config)
    with pytest.raises(FirmsError):
        execute_run(empty_engine, config, first, lambda: CLOCK, firms=client)
    with Session(empty_engine) as session:
        assert session.scalar(select(func.count()).select_from(LiveRunHead)) == 0
    later = CLOCK + timedelta(seconds=121)
    lease = take(empty_engine, config, later)

    class Weather:
        def fetch(self, request):
            with Session(empty_engine) as session, session.begin():
                session.scalar(select(Job).where(Job.id == identifier).with_for_update(nowait=True))
            if degraded:
                raise WeatherError("Synthetic weather outage")
            import httpx
            from weather_factory import payload

            from firewatch.sources.open_meteo import OpenMeteoClient
            from firewatch.sources.weather_budget import WeatherBudget

            adapter = OpenMeteoClient(
                tmp_path / "live-weather",
                WeatherBudget(tmp_path / "budget"),
                now=lambda: later,
                client=httpx.Client(
                    transport=httpx.MockTransport(
                        lambda _: httpx.Response(200, json=payload(request))
                    )
                ),
            )
            return adapter.fetch(request)

    crashed = False

    def check_unpublished(stage):
        nonlocal crashed
        with Session(empty_engine) as session:
            assert session.scalar(select(func.count()).select_from(LiveRunHead)) == 0
        if remote_storage and stage == Stage.EVIDENCE and not crashed:
            crashed = True
            raise RuntimeError("Synthetic runner shutdown before publication")

    if remote_storage:
        with pytest.raises(RuntimeError, match="runner shutdown"):
            execute_run(
                empty_engine,
                config,
                lease,
                lambda: later,
                firms=client,
                weather=Weather(),
                after_stage=check_unpublished,
            )
        # A new runner has no prior raw filesystem. Retry must restore the exact
        # frozen source pins, without changing database paths or fetching providers again.
        from pathlib import Path

        from firewatch.db.models import SourceAsset

        with Session(empty_engine) as session:
            for asset in session.scalars(select(SourceAsset)):
                Path(asset.storage_path).unlink(missing_ok=True)
        later += timedelta(seconds=121)
        lease = take(empty_engine, config, later)

    run_id = execute_run(
        empty_engine,
        config,
        lease,
        lambda: later,
        firms=client,
        weather=Weather(),
        after_stage=check_unpublished,
    )
    assert sum(product == Product.NOAA20_NRT for product, _ in client.calls) == 1
    with Session(empty_engine) as session:
        assert session.get(LiveRunHead, area_id).run_id == run_id
        run = session.get(Run, run_id)
        assert run.status == "completed"
        assert (
            "weather_source_unavailable" in run.input_asset_manifest["coverage_warnings"]
        ) == degraded
        assert session.scalar(select(func.count()).select_from(Alert)) == 1
        if remote_storage:
            from firewatch.evidence.verify import audit_artifacts

            alert = session.scalar(select(Alert))
            audit = audit_artifacts(session, alert.canonical_payload, alert.original_hash, config)
            assert audit.status == "verified"
            assert audit.scope == "referenced_sources_and_local_model"


def test_critical_outbox_is_atomic_and_idempotent(work):
    engine, config, _, _ = work
    from pydantic import SecretStr

    config.chain_enabled = True
    config.alert_registry_address = "0x" + "1" * 40
    config.chain_private_key = SecretStr("1" * 64)  # Synthetic key; never used for submission.
    lease = take(engine, config)
    run_id = execute_run(engine, config, lease, lambda: CLOCK)
    assert execute_run(engine, config, lease, lambda: CLOCK) == run_id
    with Session(engine) as session:
        assert session.scalar(select(func.count()).select_from(AnchorOutbox)) == 1


def test_completed_last_attempt_is_acknowledged_after_crash(work):
    engine, config, identifier, _ = work
    config.worker_max_attempts = 1
    lease = take(engine, config)
    execute_run(engine, config, lease, lambda: CLOCK)
    assert take(engine, config, CLOCK + timedelta(seconds=121)) is None
    with Session(engine) as session:
        assert session.get(Job, identifier).status == JobStatus.SUCCEEDED


def test_equivalent_utc_offsets_share_claim_identity(work):
    engine, config, identifier, area_id = work
    with Session(engine) as session, session.begin():
        original = session.get(Job, identifier)
        second = queued(session, area_id, assets=original.payload["asset_ids"])
        clone = session.get(Job, second)
        clone.payload = dict(clone.payload) | {"as_of": T.isoformat().replace("+00:00", "Z")}
    assert take(engine, config) is not None
    assert take(engine, config) is None


def test_all_invalid_live_source_cannot_publish_empty_results(empty_engine, tmp_path):
    from refresh_factory import FakeRefreshClient

    migrate(empty_engine)
    with Session(empty_engine) as session, session.begin():
        area = region(session)
        area.enabled = True
        queued(session, area.id, mode="live")
    config = settings(tmp_path)
    client = FakeRefreshClient(tmp_path / "invalid-live", clock=lambda: CLOCK)
    client.frp = "-1"
    lease = take(empty_engine, config)
    with pytest.raises(ValueError, match="only rejected"):
        execute_run(empty_engine, config, lease, lambda: CLOCK, firms=client)
    with Session(empty_engine) as session:
        assert session.scalar(select(func.count()).select_from(LiveRunHead)) == 0
        assert session.scalar(select(func.count()).select_from(Score)) == 0


def test_reclaimed_lease_cannot_commit_inflight_provider_result(empty_engine, tmp_path):
    from refresh_factory import FakeRefreshClient

    from firewatch.db.models import IngestionBatch

    migrate(empty_engine)
    with Session(empty_engine) as session, session.begin():
        area = region(session)
        area.enabled = True
        queued(session, area.id, mode="live")
    config = settings(tmp_path)
    clock = [CLOCK]

    class StaleClient(FakeRefreshClient):
        def fetch(self, request, **kwargs):
            raw = super().fetch(request, **kwargs)
            clock[0] += timedelta(seconds=121)
            assert take(empty_engine, config, clock[0]).attempt == 2
            return raw

    first = take(empty_engine, config)
    with pytest.raises(LeaseLost):
        execute_run(
            empty_engine,
            config,
            first,
            lambda: clock[0],
            firms=StaleClient(tmp_path / "stale", clock=lambda: CLOCK),
        )
    with Session(empty_engine) as session:
        assert session.scalar(select(func.count()).select_from(IngestionBatch)) == 0
        assert session.scalar(select(func.count()).select_from(Alert)) == 0


@pytest.mark.parametrize("status", [JobStatus.QUEUED, JobStatus.RUNNING])
def test_pipeline_worker_leaves_anchor_jobs_for_their_own_executor(work, status):
    engine, config, pipeline_id, _ = work
    anchor_id = uuid4()
    with Session(engine) as session, session.begin():
        session.add(
            Job(
                id=anchor_id,
                type=JobType.ANCHOR,
                payload={"alert_id": str(uuid4())},
                payload_hash="0" * 64,
                idempotency_key=str(anchor_id),
                status=status,
                attempts=0,
                available_at=CLOCK - timedelta(days=1),
                lease_until=CLOCK - timedelta(seconds=1) if status == JobStatus.RUNNING else None,
            )
        )
    lease = take(engine, config)
    assert lease.job_id == pipeline_id
    with Session(engine) as session:
        anchor = session.get(Job, anchor_id)
        assert anchor.status == status and anchor.attempts == 0 and anchor.error is None
