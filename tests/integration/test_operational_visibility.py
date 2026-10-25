"""Historical discovery, honest capabilities and public-read boundaries on PostgreSQL."""

from datetime import timedelta

import pytest
from conftest import migrate
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from test_read_api import TOKEN, seed
from test_schema import T, outputs, region, running
from test_worker import CLOCK, settings

from firewatch.db.models import Job, ModelVersion, Region, Run
from firewatch.pipeline.jobs import schedule
from firewatch.repositories.runs import complete_and_publish_run
from firewatch.schemas.common import JobStatus, ModelStatus, RunMode

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "metadata,source,expected",
    [
        (
            {
                "orchestration": {
                    "scenario_id": "real-history-v1",
                    "scenario_name": "Recorded replay",
                }
            },
            "orchestration",
            "real-history-v1",
        ),
        ({}, "historical_run", None),
        (None, "historical_run", None),
        ({"replay_scenario": {"id": "a" * 101}}, "historical_run", None),
        ({"replay_scenario": {"id": "https://private.invalid/key"}}, "historical_run", None),
    ],
)
def test_every_completed_replay_is_discoverable_without_rewriting_evidence(
    api, metadata, source, expected
):
    client, engine, _ = api
    with Session(engine) as session, session.begin():
        run = running(session, region(session), mode=RunMode.REPLAY)
        run.input_asset_manifest = metadata
        outputs(session, run)
        complete_and_publish_run(session, run.id, T + timedelta(hours=4))
        identifier = run.id
    result = client.get("/api/v1/replays").json()
    assert result["total"] == 1
    replay = result["items"][0]
    assert replay["scenario_id_source"] == source
    assert replay["scenario_id"] == (expected or "historical-run-" + str(identifier))
    assert not replay["scenario_configured"]
    assert replay["is_historical"]
    with Session(engine) as session:
        assert session.get(Run, identifier).input_asset_manifest == metadata


def test_wall_clock_freshness_cannot_make_old_replay_look_live(api):
    client, engine, _ = api
    ids = seed(engine)
    client.app.state.clock = lambda: T + timedelta(days=365)
    value = client.get("/api/v1/runs/" + str(ids["replay"])).json()
    assert value["is_historical"]
    assert value["freshness"]["observations_stale"] is False
    assert value["freshness"]["observations_currently_stale"] is True
    assert value["freshness"]["wall_clock_observation_age_hours"] > 8000


def test_status_exposes_missing_configuration_and_stalled_worker_without_secrets(api):
    client, engine, config = api
    ids = seed(engine)
    config.firms_map_key = None
    value = client.get("/api/v1/status").json()
    assert value["data_mode"] == "mixed"
    assert value["worker"]["state"] == "unverified"
    assert not value["worker"]["scheduler_verified"]
    assert "FIRMS_KEY_NOT_CONFIGURED" in value["live_readiness"]["blockers"]
    assert not value["live_readiness"]["configuration_ready"]
    assert value["chain_readiness"]["state"] == "disabled"
    assert not value["chain_readiness"]["independent_notarization_verified"]
    assert "PRIVATE_JOB_SECRET" not in str(value)
    with Session(engine) as session, session.begin():
        job = session.get(Job, ids["job"])
        job.status = JobStatus.QUEUED
        job.available_at = T - timedelta(days=1)
    value = client.get("/api/v1/status").json()
    assert value["worker"]["state"] == "stalled"
    assert value["worker"]["pending_jobs"] == 1
    assert "WORKER_STALLED" in value["live_readiness"]["blockers"]


def test_explicit_public_reads_and_cors_never_open_operator_writes(api):
    client, engine, config = api
    seed(engine)
    config.app_env = "production"
    config.public_reads_enabled = True
    config.cors_origins = ["http://localhost:5173"]
    assert client.get("/api/v1/regions").status_code == 200
    preflight = client.options(
        "/api/v1/regions",
        headers={
            "Origin": "http://localhost:5173",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "authorization",
        },
    )
    assert preflight.status_code == 200
    assert preflight.headers["access-control-allow-origin"] == "http://localhost:5173"
    assert (
        client.options(
            "/api/v1/regions",
            headers={
                "Origin": "https://unapproved.invalid",
                "Access-Control-Request-Method": "GET",
            },
        ).status_code
        == 400
    )
    assert client.post("/api/v1/jobs/refresh", json={}).status_code == 401
    assert client.post("/api/v1/jobs/replay", json={}).status_code == 401
    config.public_reads_enabled = False
    assert client.get("/api/v1/regions").status_code == 401
    assert (
        client.get("/api/v1/regions", headers={"Authorization": "Bearer " + TOKEN}).status_code
        == 200
    )


def test_live_configuration_never_marks_oversized_region_eligible(api):
    from pydantic import SecretStr

    client, engine, config = api
    ids = seed(engine)
    config.firms_map_key = SecretStr("synthetic-firms-test-key-1234567890")
    config.max_region_area_degrees2 = 0.1
    with Session(engine) as session, session.begin():
        session.get(Region, ids["area"]).enabled = True
    live = client.get("/api/v1/status").json()["live_readiness"]
    assert not live["configuration_ready"]
    assert live["eligible_region_ids"] == []


def test_scheduler_does_not_admit_historical_only_model_to_live_queue(empty_engine, tmp_path):
    migrate(empty_engine)
    config = settings(tmp_path, active_scorer="history-only-v1")
    with Session(empty_engine) as session, session.begin():
        area = region(session)
        area.enabled = True
        session.add(
            ModelVersion(
                version=config.active_scorer,
                artifact_path="releases/history/bundle",
                artifact_hash="a" * 64,
                feature_version="features-v1",
                status=ModelStatus.APPROVED,
                metrics={},
                target_definition={},
                training_manifest={
                    "schema_version": "model-registry-v1",
                    "approval_reference": "test-review",
                    "scope": {
                        "region": area.name,
                        "bbox": {"west": 70, "east": 74, "south": 33, "north": 36},
                        "modes": ["replay"],
                    },
                },
            )
        )
    with Session(empty_engine) as session, session.begin():
        assert schedule(session, config, CLOCK) == 0
        assert session.scalar(select(func.count()).select_from(Job)) == 0
