"""Registry migrations, atomic approval/rollback and real worker persistence on synthetic data."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import timedelta

import pytest
from conftest import migrate
from inference_factory import candidate
from registry_factory import make_candidate
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from test_worker import CLOCK, take, work  # noqa: F401

from firewatch.api.presentation import ArtifactInspector, model_metadata
from firewatch.config import Settings
from firewatch.db.models import (
    Job,
    ModelActivation,
    ModelPromotion,
    ModelVersion,
    Region,
    Run,
    Score,
)
from firewatch.ml.registry import (
    ModelRegistryError,
    promote,
    resolve_approved,
    rollback,
    selected_version,
)
from firewatch.pipeline.jobs import finish
from firewatch.pipeline.runner import execute_run
from firewatch.schemas.common import JobStatus, RunMode, Stage
from firewatch.sources.firms_normalize import digest
from firewatch.worker import process_one

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def fitted_registry():
    return candidate()


def settings(root, **overrides):
    return Settings(
        database_url="postgresql+psycopg://test:password@localhost:1/firewatch_test",
        admin_api_token="test-only-" + "x" * 40,
        model_dir=root,
        _env_file=None,
        **overrides,
    )


@pytest.fixture
def registry(empty_engine, tmp_path, fitted_registry):
    migrate(empty_engine)
    config = settings(tmp_path / "models", active_scorer="registry")
    first = make_candidate(config.model_dir, fitted_registry)
    with Session(empty_engine) as session, session.begin():
        promoted = promote(session, config.model_dir, first)
    return empty_engine, config, first, promoted


def test_live_override_preserves_registry_selection_and_scope(registry):
    engine, config, first, _ = registry
    config.live_scorer = "baseline-v1"
    with Session(engine) as session:
        assert selected_version(session, config) == first.version
        assert selected_version(session, config, mode=RunMode.REPLAY) == first.version
        assert selected_version(session, config, mode=RunMode.LIVE) == "baseline-v1"
        config.live_scorer = "registry"
        assert selected_version(session, config, mode=RunMode.LIVE) == first.version
        with pytest.raises(ModelRegistryError, match="run mode"):
            resolve_approved(session, config, first.version, mode=RunMode.LIVE)


def test_promotion_idempotence_retains_previous_and_atomic_rollback(registry, fitted_registry):
    engine, config, first, _ = registry
    second = make_candidate(config.model_dir, fitted_registry, "synthetic-forest-v2")
    with Session(engine) as session, session.begin():
        assert promote(session, config.model_dir, first)["already_active"]
        promote(session, config.model_dir, second)
        assert selected_version(session, config) == second.version
    with Session(engine) as session, session.begin():
        result = rollback(
            session,
            config,
            version=None,
            reviewer="test-operator",
            rationale="Restore previous synthetic model for validation",
        )
        assert result["version"] == first.version
    with Session(engine) as session:
        assert session.scalar(select(func.count()).select_from(ModelVersion)) == 2
        assert session.scalar(select(func.count()).select_from(ModelPromotion)) == 3
        assert selected_version(session, config) == first.version
    assert (first.release_root / "bundle/model.json").exists()
    assert (second.release_root / "bundle/model.json").exists()


def test_approval_identity_audit_and_run_pin_guards(registry):
    engine, _, first, _ = registry
    with Session(engine) as session, session.begin():
        record = session.scalar(select(ModelVersion))
        event = session.scalar(select(ModelPromotion))
        assert record.training_manifest["approval_reference"] == str(event.id)
        for statement in (
            update(ModelVersion).values(artifact_hash="0" * 64),
            delete(ModelVersion),
            update(ModelPromotion).values(reviewer="changed"),
            delete(ModelPromotion),
        ):
            with pytest.raises(IntegrityError), session.begin_nested():
                session.execute(statement)


def test_unapproved_missing_scope_and_bad_path_fail_closed(empty_engine, tmp_path):
    migrate(empty_engine)
    config = settings(tmp_path, active_scorer="registry")
    with Session(empty_engine) as session:
        with pytest.raises(ModelRegistryError):
            selected_version(session, config)
        with pytest.raises(ModelRegistryError):
            resolve_approved(session, config, "unknown-model")


def test_same_version_different_artifact_is_rejected(registry):
    engine, config, first, _ = registry
    with Session(engine) as session, session.begin():
        with pytest.raises(ModelRegistryError, match="different"):
            promote(session, config.model_dir, replace(first, bundle_sha256="0" * 64))


def test_registry_rolls_back_on_transaction_failure(registry, fitted_registry):
    engine, config, first, _ = registry
    second = make_candidate(config.model_dir, fitted_registry, "synthetic-failed-v2")
    with pytest.raises(RuntimeError), Session(engine) as session, session.begin():
        promote(session, config.model_dir, second)
        raise RuntimeError("synthetic DB transaction failure")
    with Session(engine) as session:
        assert selected_version(session, config) == first.version
        assert session.scalar(select(func.count()).select_from(ModelVersion)) == 1
    assert second.release_root.exists()  # Orphan is retained safely, never mistaken for approval.


def test_concurrent_promotions_are_serialized_and_keep_both(registry, fitted_registry):
    engine, config, _, _ = registry
    versions = [
        make_candidate(config.model_dir, fitted_registry, "synthetic-parallel-" + str(i))
        for i in range(2)
    ]

    def apply(item):
        with Session(engine) as session, session.begin():
            return promote(session, config.model_dir, item)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(apply, versions))
    assert len({r["activation_revision"] for r in results}) == 2
    with Session(engine) as session:
        pointer = session.get(ModelActivation, "default")
        assert pointer.revision == 3 and pointer.active_model_id != pointer.previous_model_id
        assert session.scalar(select(func.count()).select_from(ModelVersion)) == 3


def test_corrupt_retained_release_refuses_rollback(registry, fitted_registry):
    engine, config, first, _ = registry
    second = make_candidate(config.model_dir, fitted_registry, "synthetic-second")
    with Session(engine) as session, session.begin():
        promote(session, config.model_dir, second)
    (first.release_root / "bundle/model.json").write_text("corrupted")
    with pytest.raises(ValueError), Session(engine) as session, session.begin():
        rollback(
            session,
            config,
            version=None,
            reviewer="test-operator",
            rationale="Check corrupted files",
        )
    with Session(engine) as session:
        assert selected_version(session, config) == second.version


def prepare_ml_work(work_context, fitted, version="synthetic-worker-v1"):
    engine, config, identifier, area_id = work_context
    config.model_dir = config.raw_data_dir / "models"
    with Session(engine) as session:
        area = session.get(Region, area_id)
        scope = dict(
            region=area.name,
            bbox=dict(west=area.west, south=area.south, east=area.east, north=area.north),
        )
    prepared = make_candidate(config.model_dir, fitted, version, region=scope)
    with Session(engine) as session, session.begin():
        promote(session, config.model_dir, prepared)
        job = session.get(Job, identifier)
        job.payload = dict(job.payload) | {"scorer_version": version}
        job.payload_hash = digest(job.payload)
    return prepared


def test_worker_persists_approved_model_id_probability_and_immutable_scores(work, fitted_registry):  # noqa: F811
    engine, config, identifier, _ = work
    prepared = prepare_ml_work(work, fitted_registry)
    assert process_one(engine, config, now=lambda: CLOCK)
    with Session(engine) as session, session.begin():
        job = session.get(Job, identifier)
        assert job.status == JobStatus.SUCCEEDED
        run = session.get(Run, job.result_run_id)
        score = session.scalar(select(Score).where(Score.run_id == run.id))
        assert score.scorer_version == prepared.version and score.model_id is not None
        assert score.escalation_probability is not None and score.score_kind == "probability"
        assert run.input_asset_manifest["orchestration"]["scorer"]["model_id"] == str(
            score.model_id
        )
        assert score.explanation["warnings"]
        import json

        from firewatch.db.models import Alert

        assert score.priority == "CRITICAL"
        alert = session.scalar(select(Alert).where(Alert.run_id == run.id))
        payload = json.loads(alert.canonical_payload)
        assert float(payload["escalation_probability"]) == score.escalation_probability
        assert payload["model_id"] == str(score.model_id)
        assert payload["scorer_version"] == score.scorer_version
        with pytest.raises(IntegrityError), session.begin_nested():
            session.execute(update(Score).where(Score.id == score.id).values(priority_score=0))


def test_worker_model_failure_withholds_scores_without_baseline(work, fitted_registry):  # noqa: F811
    engine, config, identifier, _ = work
    prepared = prepare_ml_work(work, fitted_registry, "synthetic-broken-v1")
    (prepared.release_root / "bundle/model.json").write_text("corrupt")
    assert process_one(engine, config, now=lambda: CLOCK)
    with Session(engine) as session:
        job = session.get(Job, identifier)
        assert job.status == JobStatus.FAILED and job.error == "MODEL_SCORING_WITHHELD"
        run = session.get(Run, job.result_run_id)
        assert run.status == "failed" and run.stage_status["scoring"] == "failed"
        assert (
            session.scalar(select(func.count()).select_from(Score).where(Score.run_id == run.id))
            == 0
        )


def test_new_promotion_does_not_change_resumed_run_pin(work, fitted_registry):  # noqa: F811
    engine, config, identifier, _ = work
    first = prepare_ml_work(work, fitted_registry, "synthetic-resume-v1")
    lease = take(engine, config)

    def crash(stage):
        if stage == Stage.FEATURES:
            raise SystemExit("synthetic crash after features")

    with pytest.raises(SystemExit):
        execute_run(engine, config, lease, lambda: CLOCK, after_stage=crash)
    second = make_candidate(config.model_dir, fitted_registry, "synthetic-resume-v2")
    with Session(engine) as session, session.begin():
        promote(session, config.model_dir, second)
    later = CLOCK + timedelta(seconds=121)
    lease = take(engine, config, later)
    identifier = execute_run(engine, config, lease, lambda: later)
    with Session(engine) as session, session.begin():
        finish(session, lease, config, later)
        assert session.get(Run, identifier).scorer_version == first.version
        assert (
            session.scalar(select(Score).where(Score.run_id == identifier)).scorer_version
            == first.version
        )


def test_registry_api_reports_approved_bundle_ready(registry):
    engine, config, _, _ = registry
    with Session(engine) as session:
        result = model_metadata(session, config, ArtifactInspector())
        assert result.status == "ready" and result.scoring_available and result.artifact_ready
        assert result.probability_supported


def test_pending_run_pin_cannot_be_rewritten(work, fitted_registry):  # noqa: F811
    engine, config, identifier, _ = work
    prepare_ml_work(work, fitted_registry, "synthetic-pinned-v1")
    lease = take(engine, config)

    def crash(stage):
        if stage == Stage.FEATURES:
            raise SystemExit("synthetic pause")

    with pytest.raises(SystemExit):
        execute_run(engine, config, lease, lambda: CLOCK, after_stage=crash)
    with Session(engine) as session, session.begin():
        run = session.get(Run, session.get(Job, identifier).result_run_id)
        from copy import deepcopy

        changed = deepcopy(run.input_asset_manifest)
        changed["orchestration"]["scorer"]["manifest_sha256"] = "0" * 64
        with pytest.raises(IntegrityError), session.begin_nested():
            session.execute(
                update(Run).where(Run.id == run.id).values(input_asset_manifest=changed)
            )


def test_retired_model_cannot_resume_or_silently_fall_back(work, fitted_registry):  # noqa: F811
    engine, config, identifier, _ = work
    prepare_ml_work(work, fitted_registry, "synthetic-retired-v1")
    lease = take(engine, config)

    def crash(stage):
        if stage == Stage.FEATURES:
            raise SystemExit("synthetic pause")

    with pytest.raises(SystemExit):
        execute_run(engine, config, lease, lambda: CLOCK, after_stage=crash)
    with Session(engine) as session, session.begin():
        model = session.scalar(select(ModelVersion))
        model.status = "retired"
    assert process_one(engine, config, now=lambda: CLOCK + timedelta(seconds=121))
    with Session(engine) as session:
        job = session.get(Job, identifier)
        assert job.status == "failed" and job.error == "MODEL_APPROVAL_UNAVAILABLE"
        assert session.get(Run, job.result_run_id).status == "failed"
        assert session.scalar(select(func.count()).select_from(Score)) == 0


def test_required_frp_missing_withholds_entire_ml_batch(work, fitted_registry, monkeypatch):  # noqa: F811
    engine, config, identifier, _ = work
    prepare_ml_work(work, fitted_registry, "synthetic-quality-v1")
    import firewatch.pipeline.runner as runner

    original = runner.score_features

    def missing_at_scoring_boundary(features, version, **kwargs):
        rows = [feature.model_copy(deep=True) for feature in features]
        rows[0].values["frp_max_mw"] = None
        rows[0].missing_flags["frp_max_mw"] = True
        rows[0].feature_hash = digest(rows[0].model_dump(mode="json", exclude={"feature_hash"}))
        return original(rows, version, **kwargs)

    monkeypatch.setattr(runner, "score_features", missing_at_scoring_boundary)
    assert process_one(engine, config, now=lambda: CLOCK)
    with Session(engine) as session:
        job = session.get(Job, identifier)
        assert job.status == "failed" and job.error == "MODEL_SCORING_WITHHELD"
        assert session.scalar(select(func.count()).select_from(Score)) == 0


def test_scope_checks_prevent_model_use_outside_review(registry):
    engine, config, first, _ = registry
    with Session(engine) as session:
        with pytest.raises(ModelRegistryError, match="mode"):
            resolve_approved(session, config, first.version, mode=RunMode.LIVE)
        area = Region(name="unsupported-region", west=70, east=74, south=33, north=36)
        with pytest.raises(ModelRegistryError, match="region"):
            resolve_approved(session, config, first.version, region=area)
