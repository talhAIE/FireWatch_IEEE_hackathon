"""Restartable staged pipeline. Provider I/O and scoring run outside DB transactions."""

import re
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any, Literal
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from pydantic import Field, model_validator
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from firewatch.config import Settings
from firewatch.db.models import (
    Alert,
    AnchorOutbox,
    IngestionBatch,
    Job,
    Region,
    Run,
    Score,
    SourceAsset,
)
from firewatch.evidence.canonical import canonical_bytes, evidence_hash
from firewatch.evidence.files import ReadBudget
from firewatch.evidence.payload import EvidenceRegion, build_evidence, evidence_content
from firewatch.evidence.serialization import CANONICALIZATION_VERSION
from firewatch.evidence.storage import SupabaseStore, preserve_source, source_bytes
from firewatch.ml.baselines import BASELINE_VERSIONS
from firewatch.ml.inference_bundle import ApprovedScorer
from firewatch.ml.predict import _approved_runtime, score_features
from firewatch.ml.registry import ModelScoringError, resolve_approved
from firewatch.pipeline.jobs import Lease, fence
from firewatch.pipeline.replay_bundle import import_bundle, load_bundle
from firewatch.processing.clusters import ClusteringSpec
from firewatch.processing.features import FeatureSpec, load_feature_spec
from firewatch.repositories.features import EnrichmentPins, build_run_features, collect_enrichment
from firewatch.repositories.ingestion import persist_normalized
from firewatch.repositories.revisions import RevisionPolicy
from firewatch.repositories.runs import complete_and_publish_run
from firewatch.repositories.snapshots import build_run_snapshots
from firewatch.repositories.weather import cached_weather, save_weather
from firewatch.schemas.common import (
    DTO,
    Completeness,
    Digest,
    JobType,
    Priority,
    Product,
    RunMode,
    RunStatus,
    Sensor,
    Stage,
    UTCDateTime,
)
from firewatch.schemas.contracts import BoundingBox, FeatureSet, ScoreResult
from firewatch.schemas.firms_refresh import RefreshSpec
from firewatch.schemas.operator import Slug
from firewatch.sources.firms import FirmsClient
from firewatch.sources.firms_normalize import digest, normalize_firms_batch
from firewatch.sources.open_meteo import OpenMeteoClient, WeatherRequest, weather_cell
from firewatch.sources.weather_budget import WeatherError


class PipelineRequest(DTO):
    schema_version: Literal["jobs-v1"]
    region_id: UUID
    mode: RunMode
    as_of: UTCDateTime
    window_start: UTCDateTime
    window_end: UTCDateTime
    scorer_version: str
    feature_version: str
    asset_ids: list[UUID] = Field(default_factory=list, max_length=200)
    scenario_id: Slug | None = None
    scenario_name: str | None = None
    bundle_sha256: Digest | None = None

    @model_validator(mode="after")
    def validate_window(self) -> "PipelineRequest":
        if self.window_start >= self.window_end or self.window_end != self.as_of:
            raise ValueError("Invalid pipeline window")
        if self.mode == RunMode.REPLAY and not self.asset_ids:
            raise ValueError("Replay requires explicitly pinned assets")
        if (
            self.feature_version != "features-v1"
            or self.scorer_version == "registry"
            or not re.fullmatch(r"[a-z][a-z0-9._-]{0,99}", self.scorer_version)
        ):
            raise ValueError("Unsupported scorer or feature version")
        return self


def _request_manifest(request: PipelineRequest) -> dict[str, Any]:
    result = request.model_dump(mode="json")
    if result["bundle_sha256"] is None:
        result.pop("bundle_sha256")  # Preserve Phase 3.5 run compatibility.
    return result


def _quality_flags(feature: FeatureSet) -> list[str]:
    flags = feature.lineage.get("quality_flags", [])
    return [f for f in flags if isinstance(f, str)] if isinstance(flags, list) else []


def _checkpoint(run: Run, stage: Stage) -> None:
    run.stage_status = dict(run.stage_status) | {stage.value: "succeeded"}


def _run(session: Session, identifier: UUID) -> Run:
    run = session.scalar(select(Run).where(Run.id == identifier).with_for_update())
    if run is None:
        raise ValueError("Pipeline run unavailable")
    return run


def execute_run(
    engine: Engine,
    settings: Settings,
    lease: Lease,
    now: Callable[[], datetime],
    *,
    firms: FirmsClient | None = None,
    weather: OpenMeteoClient | None = None,
    after_stage: Callable[[Stage], None] = lambda _: None,
) -> UUID:
    """Each checkpoint commits independently; every output commit fences lease ownership."""
    with Session(engine) as session:
        pending = session.get(Job, lease.job_id)
        if pending is None:
            raise ValueError("Unknown replay job")
        initial = PipelineRequest.model_validate(pending.payload)
    bundle = None
    if initial.bundle_sha256:
        if initial.mode != RunMode.REPLAY or not initial.scenario_id:
            raise ValueError("A replay bundle requires an identified historical scenario")
        bundle = load_bundle(
            settings.replay_bundle_dir / initial.scenario_id, initial.bundle_sha256
        )
        with Session(engine) as imported, imported.begin():
            import_bundle(imported, settings.replay_bundle_dir / initial.scenario_id, bundle)
        if (
            bundle.scenario.id != initial.scenario_id
            or bundle.scenario.window_start != initial.window_start
            or bundle.scenario.window_end != initial.as_of
            or set(bundle.scenario.asset_ids) != set(initial.asset_ids)
            or bundle.scenario.scorer_version != initial.scorer_version
            or bundle.scenario.feature_version != initial.feature_version
        ):
            raise ValueError("Replay request differs from its frozen bundle")
    with Session(engine) as session, session.begin():
        job = fence(session, lease, now())
        if job.type not in {JobType.REFRESH, JobType.REPLAY}:
            raise ValueError("Unsupported pipeline job type")
        request = PipelineRequest.model_validate(job.payload)
        if (job.type == JobType.REFRESH) != (request.mode == RunMode.LIVE):
            raise ValueError("Job type and run mode disagree")
        if request.as_of > now():
            raise ValueError("Pipeline cutoff cannot be in the future")
        area = session.get(Region, request.region_id)
        if area is None or (not area.enabled and bundle is None):
            raise ValueError("Pipeline requires an enabled region or a verified replay bundle")
        if bundle is not None and area.name != bundle.scenario.region_name:
            raise ValueError("Replay region differs from its frozen bundle")
        if (area.east - area.west) * (area.north - area.south) > settings.max_region_area_degrees2:
            raise ValueError("Region exceeds configured bound")
        bbox = BoundingBox(west=area.west, south=area.south, east=area.east, north=area.north)
        region_name = area.name
        key = f"pipeline-v1:{area.id}:{request.mode}:{request.as_of.isoformat()}"
        if bundle is not None:
            key = f"replay-v1:{area.id}:{request.bundle_sha256}:{request.as_of.isoformat()}"
        identifier = uuid5(NAMESPACE_URL, key)
        # All workers first lock the job then the run, including duplicate requests.
        run = session.get(Run, identifier)
        if run is None:
            config = (
                bundle.feature_config.model_copy(deep=True)
                if bundle is not None
                else load_feature_spec(settings.features_config)
            )
            if bundle is None and (
                settings.cluster_radius_km != 2
                or settings.snapshot_lookback_hours != 6
                or settings.weather_cache_grid_degrees != 0.1
            ):
                raise ValueError("features-v1 requires the frozen cluster/grid definition")
            if bundle is None:
                config.weather_max_age_hours = settings.weather_max_age_hours
            frozen: dict[str, Any] = dict(
                request=_request_manifest(request),
                bbox=bbox.model_dump(),
                features=config.model_dump(mode="json"),
                evidence=settings.evidence_schema_version,
                evidence_canonicalization=CANONICALIZATION_VERSION,
                region=dict(region_id=str(area.id), name=region_name, bbox=bbox.model_dump()),
                detection_max_age_hours=(
                    bundle.detection_max_age_hours
                    if bundle is not None
                    else settings.detection_max_age_hours
                ),
                network=f"eip155:{settings.chain_id}:{settings.alert_registry_address.lower()}"
                if settings.chain_enabled and bundle is None
                else None,
            )
            if request.scorer_version not in BASELINE_VERSIONS:
                approved = resolve_approved(
                    session, settings, request.scorer_version, region=area, mode=request.mode
                )
                frozen["scorer"] = approved.model_dump(mode="json")
            if frozen["evidence"] != "evidence-v1":
                raise ValueError("Unsupported evidence schema")
            run = Run(
                id=identifier,
                region_id=area.id,
                mode=request.mode,
                as_of=request.as_of,
                run_key=key,
                config_hash=digest(frozen),
                status=RunStatus.RUNNING,
                scorer_version=request.scorer_version,
                feature_version=request.feature_version,
                started_at=now(),
                stage_status={},
                completeness=Completeness.UNAVAILABLE,
                input_asset_manifest={"orchestration": frozen},
            )
            session.add(run)
            session.flush()
        else:
            run = _run(session, identifier)
            frozen = run.input_asset_manifest["orchestration"]
            if (
                digest(frozen) != run.config_hash
                or frozen["bbox"] != bbox.model_dump()
                or frozen["request"] != _request_manifest(request)
            ):
                raise ValueError("Run key conflicts with changed inputs or configuration")
            config = FeatureSpec.model_validate(frozen["features"])
            if run.status == RunStatus.FAILED:
                run.status = RunStatus.RUNNING
        job.result_run_id = identifier
        if run.status == RunStatus.COMPLETED:
            return identifier

    if frozen.get("evidence_canonicalization") not in {None, CANONICALIZATION_VERSION}:
        raise ValueError("Unsupported saved evidence canonicalization")

    approved_reference = None
    if "scorer" in frozen:
        approved_reference = ApprovedScorer.model_validate(frozen["scorer"])
        with Session(engine) as session:
            current = resolve_approved(
                session,
                settings,
                request.scorer_version,
                region=session.get(Region, request.region_id),
                mode=request.mode,
            )
        if current != approved_reference:
            raise ModelScoringError("Saved approval no longer matches the registry")

    # Ingest one provider chunk at a time, retaining completed chunks after a crash.
    if request.mode == RunMode.LIVE:
        spec = RefreshSpec(region=region_name, as_of=request.as_of)
        for index, source_request in enumerate(spec.requests(bbox)):
            with Session(engine) as session:
                run = session.get(Run, identifier)
                assert run is not None
                done = run.input_asset_manifest.get("ingest_chunks", {})
                if str(index) in done:
                    continue
            if firms is None:
                raise ValueError("FIRMS client is required for pending live ingestion")
            if now() - request.as_of > timedelta(hours=settings.max_refresh_age_hours):
                raise ValueError("Saved live cutoff is too old to fetch")
            # A fresh fetch ID avoids concurrent stale-attempt filesystem collisions.
            raw = firms.fetch(source_request, batch_id=uuid4())
            normalized = normalize_firms_batch(raw)
            preserve_source(normalized.asset.storage_path, normalized.asset.content_hash, settings)
            with Session(engine) as session, session.begin():
                fence(session, lease, now())
                run = _run(session, identifier)
                ingested = persist_normalized(session, normalized, request.region_id)
                chunks = dict(run.input_asset_manifest.get("ingest_chunks", {}))
                chunks[str(index)] = str(ingested.batch_id)
                run.input_asset_manifest = dict(run.input_asset_manifest) | {
                    "ingest_chunks": chunks
                }

    with Session(engine) as session, session.begin():
        fence(session, lease, now())
        run = _run(session, identifier)
        if run.stage_status.get(Stage.INGEST.value) != "succeeded":
            if request.mode == RunMode.LIVE:
                ids = list(run.input_asset_manifest["ingest_chunks"].values())
                products = {Sensor.NOAA20: Product.NOAA20_NRT, Sensor.NOAA21: Product.NOAA21_NRT}
            else:
                batches = list(
                    session.scalars(
                        select(IngestionBatch).where(
                            IngestionBatch.region_id == request.region_id,
                            IngestionBatch.asset_id.in_(request.asset_ids),
                        )
                    )
                )
                if {b.asset_id for b in batches} != set(request.asset_ids):
                    raise ValueError("Pinned replay satellite assets are unavailable")
                ids = [str(b.id) for b in batches]
                products = {}
                for b in batches:
                    sensor = Sensor.NOAA20 if "NOAA20" in b.product else Sensor.NOAA21
                    if sensor in products and products[sensor] != b.product:
                        raise ValueError("Replay mixes different products for one sensor")
                    products[sensor] = b.product
            policy = RevisionPolicy(
                products=products,
                batch_ids=ids,
                retrieval_cutoff=now(),
                allow_rejected_batches=True,
            )
            chosen = list(
                session.scalars(
                    select(IngestionBatch).where(IngestionBatch.id.in_(policy.batch_ids))
                )
            )
            if any(
                b.fetched_count > 0 and b.accepted_count + b.duplicate_count == 0 for b in chosen
            ):
                raise ValueError("A satellite source contains only rejected rows")
            if bundle is not None:
                policy = bundle.policy
            # Validate complete source days before claiming ingest success.
            from firewatch.repositories.snapshots import check_coverage

            check_coverage(session, run, policy)
            run.input_asset_manifest = dict(run.input_asset_manifest) | {
                "ingestion_policy": policy.model_dump(mode="json")
            }
            _checkpoint(run, Stage.INGEST)
    after_stage(Stage.INGEST)

    with Session(engine) as session, session.begin():
        fence(session, lease, now())
        run = _run(session, identifier)
        policy = RevisionPolicy.model_validate(run.input_asset_manifest["ingestion_policy"])
        snapshots = build_run_snapshots(
            session,
            identifier,
            policy,
            bundle.clustering_config if bundle is not None else ClusteringSpec(),
        )
    after_stage(Stage.SNAPSHOTS)

    # Climate is precomputed. Live weather fetches only bounded missing cells, without locks.
    with Session(engine) as session:
        run = session.get(Run, identifier)
        assert run is not None
        need_enrichment = run.stage_status.get(Stage.ENRICHMENT.value) != "succeeded"
        weather_warning = "weather_source_unavailable" in run.input_asset_manifest.get(
            "coverage_warnings", []
        )
    if need_enrichment and request.mode == RunMode.LIVE:
        cells = {
            weather_cell(d.snapshot.latitude, d.snapshot.longitude).key: weather_cell(
                d.snapshot.latitude, d.snapshot.longitude
            )
            for d in snapshots.snapshots
        }
        if len(cells) > config.max_weather_cells:
            raise ValueError("Weather cell bound exceeded")
        end = request.as_of.replace(minute=0, second=0, microsecond=0)
        for cell in cells.values():
            wr = WeatherRequest(
                cells=[cell],
                start=end - timedelta(hours=23),
                end=end,
                product="live",
                model="gfs_seamless",
                mode=RunMode.LIVE,
                source_version=config.weather_namespace,
            )
            with Session(engine) as session:
                cached = cached_weather(
                    session,
                    wr,
                    now(),
                    oldest_retrieval=now() - timedelta(hours=config.weather_max_age_hours),
                )
            if cached is not None:
                continue
            try:
                if weather is None:
                    raise WeatherError("Live weather client unavailable")
                batch = weather.fetch(wr)
                preserve_source(batch.asset.storage_path, batch.asset.content_hash, settings)
                with Session(engine) as session, session.begin():
                    fence(session, lease, now())
                    save_weather(session, batch)
            except WeatherError:
                weather_warning = True  # Missing inputs remain null and quality flagged.

    prepared_pins = None
    prepared_sources = None
    availability = None
    if settings.supabase_storage_enabled and request.mode == RunMode.LIVE:
        # Choose/fetch immutable pins before taking a Run write lock. No provider or
        # storage I/O occurs inside the feature transaction, including cross-host retries.
        with Session(engine) as session:
            saved = session.get(Run, identifier)
            assert saved is not None
            if "features" in saved.input_asset_manifest:
                prepared_pins = EnrichmentPins.model_validate(
                    saved.input_asset_manifest["features"]["pins"]
                )
            else:
                availability = now()
                prepared_pins = collect_enrichment(
                    session, saved, snapshots, policy, config, availability
                )
            refs = prepared_pins.assets + prepared_pins.satellite_assets
            if len(refs) > 256:
                raise ValueError("Live source reference bound exceeded")
            assets = {pin.asset_id: session.get(SourceAsset, pin.asset_id) for pin in refs}
        prepared_sources = {}
        budget = ReadBudget()
        with SupabaseStore(settings) as store:
            for pin in refs:
                asset = assets[pin.asset_id]
                if asset is None or asset.content_hash != pin.sha256:
                    raise ValueError("Live source identity changed")
                content = source_bytes(asset.storage_path, pin.sha256, settings, budget)
                store.put(pin.sha256, content)
                prepared_sources[pin.asset_id] = content

    with Session(engine) as session, session.begin():
        fence(session, lease, now())
        run = _run(session, identifier)
        availability = availability or (
            bundle.pins.retrieval_cutoff
            if bundle is not None
            else None
            if "features" in run.input_asset_manifest
            else now()
        )
        features = build_run_features(
            session,
            identifier,
            config,
            retrieval_cutoff=availability,
            enrichment_pins=bundle.pins if bundle is not None else prepared_pins,
            source_contents=prepared_sources,
        )
        run = _run(session, identifier)
        quality = set(snapshots.quality_flags) | {
            flag for f in features for flag in _quality_flags(f)
        }
        informational = {
            "precipitation_24h_ends_at_completed_hour",
            "hourly_model_values",
            "precipitation_preceding_hour",
            "provider_interpolation_unknown",
            "retrospective_weather",
        }
        warnings = sorted(
            (quality - informational)
            | {
                name + "_missing"
                for f in features
                for name, missing in f.missing_flags.items()
                if missing
            }
        )
        if weather_warning:
            warnings = sorted(set(warnings) | {"weather_source_unavailable"})
        latest_detection = max(
            (d.snapshot.latest_detection_at for d in snapshots.snapshots), default=None
        )
        if latest_detection and (request.as_of - latest_detection).total_seconds() / 3600 > float(
            frozen["detection_max_age_hours"]
        ):
            warnings = sorted(set(warnings) | {"satellite_observations_stale"})
        pins = run.input_asset_manifest["features"]["pins"]
        from firewatch.processing.features import FeatureValues

        valid = [
            request.as_of - timedelta(hours=age)
            for f in features
            if (age := FeatureValues.model_validate(f.values).weather_age_hours) is not None
        ]
        source_ids = {UUID(p["asset_id"]) for p in pins["assets"] + pins["satellite_assets"]}
        retrieved = list(
            session.scalars(select(SourceAsset.retrieved_at).where(SourceAsset.id.in_(source_ids)))
        )
        run.completeness = Completeness.PARTIAL if warnings else Completeness.COMPLETE
        run.input_asset_manifest = dict(run.input_asset_manifest) | {
            "coverage_warnings": warnings,
            "quality_flags": sorted(quality),
            "freshness": {
                "latest_weather_valid_at": max(valid).isoformat() if valid else None,
                "latest_retrieved_at": max(retrieved).isoformat() if retrieved else None,
            },
        }
    after_stage(Stage.ENRICHMENT)
    after_stage(Stage.FEATURES)

    # Model/scorer computation is deliberately outside any DB transaction or lock.
    with Session(engine) as session:
        run = session.get(Run, identifier)
        assert run is not None
        scoring_done = run.stage_status.get(Stage.SCORING.value) == "succeeded"
    if not scoring_done:
        scoring_time = now()
        try:
            if approved_reference is not None:
                _approved_runtime(approved_reference)
            scores = [
                score
                for start in range(0, len(features), 256)
                for score in score_features(
                    features[start : start + 256],
                    request.scorer_version,
                    scored_at=scoring_time,
                    approved=approved_reference,
                )
            ]
        except (ValueError, OSError, OverflowError, ArithmeticError):
            if approved_reference is not None:
                raise ModelScoringError(
                    "Approved model or required inputs failed; scoring withheld"
                ) from None
            raise
        with Session(engine) as session, session.begin():
            fence(session, lease, now())
            run = _run(session, identifier)
            if approved_reference is not None:
                current = resolve_approved(session, settings, request.scorer_version)
                if current != approved_reference:
                    raise ModelScoringError("Approval changed before saving scores")
            for result in scores:
                session.add(
                    Score(
                        id=uuid5(identifier, "score:" + str(result.snapshot_id)),
                        snapshot_id=result.snapshot_id,
                        run_id=identifier,
                        scorer_version=result.scorer_version,
                        model_id=result.model_id,
                        priority_score=result.priority_score,
                        score_kind=result.score_kind,
                        escalation_probability=result.escalation_probability,
                        priority=result.priority,
                        explanation=result.explanation,
                        scored_at=result.scored_at,
                    )
                )
            _checkpoint(run, Stage.SCORING)
    after_stage(Stage.SCORING)

    with Session(engine) as session, session.begin():
        fence(session, lease, now())
        run = _run(session, identifier)
        if run.stage_status.get(Stage.EVIDENCE.value) != "succeeded":
            feature_by_id = {f.snapshot_id: f for f in features}
            snapshot_by_id = {d.snapshot.snapshot_id: d.snapshot for d in snapshots.snapshots}
            formal_pins = (
                EnrichmentPins.model_validate(run.input_asset_manifest["features"]["pins"])
                if frozen.get("evidence_canonicalization") == CANONICALIZATION_VERSION
                else None
            )
            for score in session.scalars(
                select(Score).where(Score.run_id == identifier, Score.priority == Priority.CRITICAL)
            ):
                feature = feature_by_id[score.snapshot_id]
                alert_id = uuid5(score.id, "evidence-v1")
                if formal_pins is not None:
                    formal = build_evidence(
                        score_id=score.id,
                        score=ScoreResult(
                            run_id=run.id,
                            snapshot_id=score.snapshot_id,
                            mode=run.mode,
                            as_of=run.as_of,
                            feature_version=run.feature_version,
                            scorer_version=score.scorer_version,
                            model_id=score.model_id,
                            score_kind=score.score_kind,
                            priority_score=score.priority_score,
                            escalation_probability=score.escalation_probability,
                            priority=score.priority,
                            explanation=score.explanation,
                            reasons=score.explanation.get("reasons", []),
                            warnings=score.explanation.get("warnings", []),
                            scored_at=score.scored_at,
                        ),
                        snapshot=snapshot_by_id[score.snapshot_id],
                        feature=feature,
                        region=EvidenceRegion.model_validate(frozen["region"]),
                        firms=run.input_asset_manifest["firms"],
                        pins=formal_pins,
                        run_config_hash=run.config_hash,
                        feature_stage_hash=run.input_asset_manifest["features"]["sha256"],
                        warnings=run.input_asset_manifest.get("coverage_warnings", []),
                        approved=approved_reference,
                    )
                    content, original_hash = evidence_content(formal)
                else:
                    # Runs admitted before Phase 5.1 keep their original exact encoding.
                    payload = dict(
                        schema_version="evidence-v1",
                        alert_id=str(alert_id),
                        run_id=str(identifier),
                        mode=request.mode.value,
                        as_of=request.as_of.isoformat(),
                        snapshot_id=str(score.snapshot_id),
                        score_id=str(score.id),
                        scorer_version=score.scorer_version,
                        priority_score=score.priority_score,
                        priority=score.priority.value,
                        score_kind=score.score_kind.value,
                        escalation_probability=score.escalation_probability,
                        scored_at=score.scored_at.isoformat(),
                        explanation=score.explanation,
                        feature=feature.model_dump(mode="json"),
                        source_manifest={
                            "satellite": run.input_asset_manifest["firms"],
                            "enrichment": run.input_asset_manifest["features"]["pins"],
                        },
                    )
                    if score.model_id is not None:
                        payload["model_id"] = str(score.model_id)
                    content = canonical_bytes(payload)
                    original_hash = evidence_hash(content)
                session.add(
                    Alert(
                        id=alert_id,
                        snapshot_id=score.snapshot_id,
                        run_id=identifier,
                        score_id=score.id,
                        canonical_payload=content,
                        original_hash=original_hash,
                        evidence_schema_version="evidence-v1",
                        created_at=now(),
                    )
                )
                session.flush()
                if frozen["network"]:
                    session.add(
                        AnchorOutbox(
                            id=uuid5(alert_id, frozen["network"]),
                            alert_id=alert_id,
                            network=frozen["network"],
                            available_at=now(),
                        )
                    )
            _checkpoint(run, Stage.EVIDENCE)
    after_stage(Stage.EVIDENCE)

    with Session(engine) as session, session.begin():
        fence(session, lease, now())
        complete_and_publish_run(session, identifier, now())
    return identifier
