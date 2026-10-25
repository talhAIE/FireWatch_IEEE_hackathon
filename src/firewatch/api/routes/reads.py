"""Phase 1.4 public read endpoints and authenticated job status."""

import re
from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

import yaml
from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import func, select, text

from firewatch.api.dependencies import (
    ReadSession,
    api_settings,
    public_read_policy,
    require_operator,
)
from firewatch.api.errors import APIError
from firewatch.api.operations import operational_status
from firewatch.api.presentation import (
    context,
    evidence_projection,
    hotspot,
    model_metadata,
    public_object,
    run_view,
    safe_text,
    stages,
    verification,
)
from firewatch.db.base import SCHEMA_REVISION
from firewatch.db.models import (
    Alert,
    ClimateBaselineRecord,
    ClusterSnapshotRecord,
    Detection,
    DetectionRegion,
    Job,
    LiveRunHead,
    Region,
    Run,
    WeatherRecord,
)
from firewatch.repositories.reads import (
    hotspot_selection,
    page_run_times,
    ranked_page,
    region_rows,
    require_run,
    run_selection,
    source_attempts,
)
from firewatch.repositories.refresh import latest_refreshes
from firewatch.schemas.api import (
    AlertView,
    ErrorEnvelope,
    FeatureCollection,
    GeoJSONFeature,
    GeoJSONQuery,
    Geometry,
    HotspotDetail,
    HotspotFilters,
    HotspotPage,
    HotspotQuery,
    JobView,
    ModelMetadata,
    Page,
    Pagination,
    Readiness,
    RegionRefresh,
    RegionView,
    ReplayView,
    RunQuery,
    RunView,
    ScopeQuery,
    SourceStatus,
    StatusView,
    Summary,
    Verification,
    VerificationQuery,
)
from firewatch.schemas.common import (
    JobStatus,
    Priority,
    Product,
    Provider,
    RunMode,
    RunStatus,
    ScoreKind,
)
from firewatch.schemas.contracts import BoundingBox
from firewatch.schemas.firms_refresh import summarize_refresh
from firewatch.schemas.operator import ReplayConfig

ERRORS: dict[int | str, dict[str, Any]] = {
    code: {"model": ErrorEnvelope} for code in (401, 404, 422, 503)
}
router = APIRouter(prefix="/api/v1", dependencies=[Depends(public_read_policy)], responses=ERRORS)
health = APIRouter(responses={503: {"model": ErrorEnvelope}})
operator = APIRouter(prefix="/api/v1", dependencies=[Depends(require_operator)], responses=ERRORS)


def now(request: Request) -> datetime:
    return request.app.state.clock()  # type: ignore[no-any-return]


def model_info(request: Request, session: ReadSession) -> ModelMetadata:
    return model_metadata(session, api_settings(request), request.app.state.artifacts)


@health.get("/health/ready", response_model=Readiness, operation_id="health_ready")
def readiness(request: Request, session: ReadSession) -> Readiness:
    versions = set(session.scalars(text("SELECT version_num FROM alembic_version")).all())
    if versions != {SCHEMA_REVISION}:
        raise APIError(503, "SCHEMA_NOT_READY", "Database migrations are not current.", True)
    metadata = model_info(request, session)
    if not metadata.artifact_ready or not metadata.scoring_available:
        raise APIError(
            503, "ARTIFACT_UNAVAILABLE", "A required local scorer artifact is unavailable.", True
        )
    return Readiness(schema_revision=SCHEMA_REVISION, artifact_ready=True)


@router.get("/regions", response_model=Page[RegionView], operation_id="list_regions")
def regions(
    request: Request, session: ReadSession, query: Annotated[Pagination, Query()]
) -> Page[RegionView]:
    records = region_rows(session)
    modes: dict[UUID, list[RunMode]] = {}
    for region_id, mode in session.execute(
        select(Run.region_id, Run.mode).where(Run.status == RunStatus.COMPLETED).distinct()
    ):
        modes.setdefault(region_id, []).append(mode)
    items = [
        RegionView(
            region_id=region.id,
            name=region.name,
            bbox=BoundingBox(
                west=region.west, south=region.south, east=region.east, north=region.north
            ),
            enabled=region.enabled,
            available_modes=sorted(modes.get(region.id, [])),
            latest_live_run_id=latest,
        )
        for region, latest in records[query.offset : query.offset + query.limit]
    ]
    return Page[RegionView](items=items, total=len(records), limit=query.limit, offset=query.offset)


@router.get("/runs", response_model=Page[RunView], operation_id="list_runs")
def runs(
    request: Request, session: ReadSession, query: Annotated[RunQuery, Query()]
) -> Page[RunView]:
    if query.region_id is not None and session.get(Region, query.region_id) is None:
        raise APIError(404, "REGION_NOT_FOUND", "The requested region was not found.")
    selection = run_selection(query)
    total = session.scalar(select(func.count()).select_from(selection.subquery())) or 0
    records = session.scalars(
        selection.order_by(Run.as_of.desc(), Run.id).limit(query.limit).offset(query.offset)
    ).all()
    times = page_run_times(session, records)
    return Page[RunView](
        items=[
            run_view(session, run, now(request), api_settings(request), times=times[run.id])
            for run in records
        ],
        total=total,
        limit=query.limit,
        offset=query.offset,
    )


@router.get("/runs/{run_id}", response_model=RunView, operation_id="get_run")
def run_detail(run_id: UUID, request: Request, session: ReadSession) -> RunView:
    return run_view(
        session, require_run(session, run_id, completed=False), now(request), api_settings(request)
    )


@router.get("/hotspots", response_model=HotspotPage, operation_id="list_hotspots")
def hotspots(
    request: Request, session: ReadSession, query: Annotated[HotspotQuery, Query()]
) -> HotspotPage:
    run = require_run(session, query.run_id)
    total, rows = ranked_page(session, hotspot_selection(run, query), query.limit, query.offset)
    return HotspotPage(
        **context(session, run, now(request), api_settings(request)).model_dump(),
        items=[hotspot(row) for row in rows],
        total=total,
        limit=query.limit,
        offset=query.offset,
    )


# Register the literal GeoJSON route before the UUID path route.
@router.get(
    "/hotspots.geojson", response_model=FeatureCollection, operation_id="get_hotspots_geojson"
)
def geojson(
    request: Request, session: ReadSession, query: Annotated[GeoJSONQuery, Query()]
) -> FeatureCollection:
    run = require_run(session, query.run_id)
    total, rows = ranked_page(session, hotspot_selection(run, query), query.limit, query.offset)
    items = [hotspot(row) for row in rows]
    return FeatureCollection(
        **context(session, run, now(request), api_settings(request)).model_dump(),
        features=[
            GeoJSONFeature(
                id=item.snapshot_id,
                geometry=Geometry(coordinates=(item.longitude, item.latitude)),
                properties=item,
            )
            for item in items
        ],
        total=total,
        returned=len(items),
        limit=query.limit,
        offset=query.offset,
        truncated=query.offset > 0 or query.offset + len(items) < total,
    )


@router.get("/hotspots/{snapshot_id}", response_model=HotspotDetail, operation_id="get_hotspot")
def hotspot_detail(
    snapshot_id: UUID, request: Request, session: ReadSession, query: Annotated[ScopeQuery, Query()]
) -> HotspotDetail:
    record = session.get(ClusterSnapshotRecord, snapshot_id)
    if record is None or (query.run_id is not None and query.run_id != record.run_id):
        raise APIError(404, "HOTSPOT_NOT_FOUND", "The requested hotspot was not found in this run.")
    run = require_run(session, record.run_id)
    row = session.execute(
        hotspot_selection(run, HotspotFilters(run_id=run.id)).where(
            ClusterSnapshotRecord.id == snapshot_id
        )
    ).one_or_none()
    if row is None:
        raise APIError(
            503, "OUTPUT_UNAVAILABLE", "The completed hotspot output is unavailable.", True
        )
    features = row[2]
    missing = {
        safe_text(key): value
        for key, value in features.missing_flags.items()
        if isinstance(value, bool)
    }
    units = {
        safe_text(key): safe_text(value)
        for key, value in features.units.items()
        if isinstance(value, str)
    }
    return HotspotDetail(
        **context(session, run, now(request), api_settings(request)).model_dump(),
        hotspot=hotspot(row),
        algorithm_version=record.algorithm_version,
        features=public_object(features.values),
        missing_flags=missing,
        units=units,
        feature_hash=features.feature_hash,
        explanation=public_object(row[1].explanation),
        evidence=alert_detail(row[3], request, session, VerificationQuery(run_id=run.id))
        if row[3] is not None
        else None,
    )


@router.get("/summary", response_model=Summary, operation_id="get_summary")
def summary(
    request: Request, session: ReadSession, query: Annotated[HotspotFilters, Query()]
) -> Summary:
    run = require_run(session, query.run_id)
    selection = hotspot_selection(run, query).subquery()
    counts = session.execute(
        select(selection.c.priority, selection.c.score_kind, func.count()).group_by(
            selection.c.priority, selection.c.score_kind
        )
    ).all()
    priorities = dict.fromkeys(Priority, 0)
    kinds = dict.fromkeys(ScoreKind, 0)
    for priority, kind, count in counts:
        priorities[priority] += count
        kinds[kind] += count
    return Summary(
        **context(session, run, now(request), api_settings(request)).model_dump(),
        total=sum(priorities.values()),
        priority_counts=priorities,
        score_kind_counts=kinds,
        filters=query,
    )


@router.get("/models/current", response_model=ModelMetadata, operation_id="get_current_model")
def current_model(request: Request, session: ReadSession) -> ModelMetadata:
    return model_info(request, session)


@router.get("/replays", response_model=Page[ReplayView], operation_id="list_replays")
def replays(
    request: Request, session: ReadSession, query: Annotated[Pagination, Query()]
) -> Page[ReplayView]:
    selection = select(Run).where(
        Run.mode == RunMode.REPLAY,
        Run.status == RunStatus.COMPLETED,
    )
    total = session.scalar(select(func.count()).select_from(selection.subquery())) or 0
    records = session.scalars(
        selection.order_by(Run.as_of.desc(), Run.id).limit(query.limit).offset(query.offset)
    ).all()
    times = page_run_times(session, records)
    try:
        with api_settings(request).replays_config.open("rb") as stream:
            raw = stream.read(256 * 1024 + 1)
        if len(raw) > 256 * 1024:
            raise ValueError("Oversized replay configuration")
        configured = {s.id for s in ReplayConfig.model_validate(yaml.safe_load(raw)).scenarios}
    except (OSError, ValueError, RecursionError, yaml.YAMLError):
        configured = set()
    items = []
    for run in records:
        manifest = run.input_asset_manifest if isinstance(run.input_asset_manifest, dict) else {}
        scenario = manifest.get("replay_scenario", {})
        scenario = scenario if isinstance(scenario, dict) else {}
        orchestration = manifest.get("orchestration", {})
        orchestration = orchestration if isinstance(orchestration, dict) else {}
        identifier, source = scenario.get("id"), "manifest"
        if (
            not isinstance(identifier, str)
            or len(identifier) > 100
            or not re.fullmatch(r"[a-z0-9]+(-[a-z0-9]+)*", identifier)
        ):
            identifier, source = orchestration.get("scenario_id"), "orchestration"
        if (
            not isinstance(identifier, str)
            or len(identifier) > 100
            or not re.fullmatch(r"[a-z0-9]+(-[a-z0-9]+)*", identifier)
        ):
            identifier, source = "historical-run-" + str(run.id), "historical_run"
        items.append(
            ReplayView(
                **context(
                    session, run, now(request), api_settings(request), times=times[run.id]
                ).model_dump(),
                scenario_id=identifier,
                scenario_id_source=source,
                scenario_configured=identifier in configured,
                name=safe_text(
                    scenario.get("name") or orchestration.get("scenario_name"), identifier
                ),
                description=safe_text(scenario.get("description")),
                region_id=run.region_id,
                scorer_version=run.scorer_version,
                feature_version=run.feature_version,
            )
        )
    return Page[ReplayView](items=items, total=total, limit=query.limit, offset=query.offset)


def saved_alert(alert_id: UUID, query: ScopeQuery, session: ReadSession) -> tuple[Alert, Run]:
    alert = session.get(Alert, alert_id)
    if alert is None or (query.run_id is not None and query.run_id != alert.run_id):
        raise APIError(404, "ALERT_NOT_FOUND", "The requested alert was not found in this run.")
    return alert, require_run(session, alert.run_id)


@router.get("/alerts/{alert_id}", response_model=AlertView, operation_id="get_alert")
def alert_detail(
    alert_id: UUID,
    request: Request,
    session: ReadSession,
    query: Annotated[VerificationQuery, Query()],
) -> AlertView:
    alert, run = saved_alert(alert_id, query, session)
    payload, redacted = evidence_projection(alert)
    return AlertView(
        **context(session, run, now(request), api_settings(request)).model_dump(),
        alert_id=alert.id,
        snapshot_id=alert.snapshot_id,
        score_id=alert.score_id,
        evidence_schema_version=alert.evidence_schema_version,
        created_at=alert.created_at,
        original_hash=alert.original_hash,
        payload=payload,
        payload_redacted=redacted,
        verification=verification(
            session,
            alert,
            run,
            now(request),
            api_settings(request),
            request.app.state.anchor_reader,
            verify_artifacts=query.verify_artifacts,
        ),
    )


@router.get("/alerts/{alert_id}/verify", response_model=Verification, operation_id="verify_alert")
def verify_alert(
    alert_id: UUID,
    request: Request,
    session: ReadSession,
    query: Annotated[VerificationQuery, Query()],
) -> Verification:
    alert, run = saved_alert(alert_id, query, session)
    return verification(
        session,
        alert,
        run,
        now(request),
        api_settings(request),
        request.app.state.anchor_reader,
        verify_artifacts=query.verify_artifacts,
    )


@operator.get("/jobs/{job_id}", response_model=JobView, operation_id="get_job")
def job_detail(job_id: UUID, session: ReadSession) -> JobView:
    job = session.get(Job, job_id)
    if job is None:
        raise APIError(404, "JOB_NOT_FOUND", "The requested job was not found.")
    run = session.get(Run, job.result_run_id) if job.result_run_id else None
    return JobView(
        job_id=job.id,
        type=job.type,
        status=job.status,
        attempts=job.attempts,
        available_at=job.available_at,
        lease_until=job.lease_until,
        heartbeat_at=job.heartbeat_at,
        result_run_id=job.result_run_id,
        stages=stages(run) if run else {},
        failure="Job execution failed; inspect private logs using the job ID."
        if job.error
        else None,
    )


@router.get("/status", response_model=StatusView, operation_id="get_status")
def status(request: Request, session: ReadSession) -> StatusView:
    clock = now(request)
    settings = api_settings(request)
    regions = region_rows(session)
    attempts, successes = source_attempts(session)
    refresh_states = latest_refreshes(session, clock)
    latest = {(batch.region_id, batch.product): (batch, retrieved) for batch, retrieved in attempts}
    successful = {(region, product): retrieved for region, product, retrieved in successes}
    acquisitions = {
        (region, product): acquired
        for region, product, acquired in session.execute(
            select(DetectionRegion.region_id, Detection.product, func.max(Detection.acquired_at))
            .join(Detection, Detection.id == DetectionRegion.detection_id)
            .where(Detection.acquired_at <= clock)
            .group_by(DetectionRegion.region_id, Detection.product)
        )
    }
    heads = {
        row.id: row
        for row in session.scalars(select(Run).join(LiveRunHead, LiveRunHead.run_id == Run.id))
    }
    sources: list[SourceStatus] = []
    refreshes: list[RegionRefresh] = []
    for region, head_id in regions:
        refreshes.append(
            RegionRefresh(
                region_id=region.id,
                latest_live_run_id=head_id,
                last_successful_refresh_at=heads[head_id].completed_at if head_id else None,
            )
        )
        for product in (Product.NOAA20_NRT, Product.NOAA21_NRT):
            attempt = latest.get((region.id, product))
            success = successful.get((region.id, product))
            acquisition = acquisitions.get((region.id, product))
            source_state = "never_fetched"
            if attempt:
                if attempt[0].status != JobStatus.SUCCEEDED:
                    source_state = "degraded" if success else "unavailable"
                elif (clock - attempt[1]).total_seconds() > settings.detection_max_age_hours * 3600:
                    source_state = "stale"
                else:
                    source_state = "ok"
            progress = refresh_states.get((region.id, product))
            source_progress = (
                next(
                    (
                        item
                        for item in summarize_refresh(progress).products
                        if item.product == product
                    ),
                    None,
                )
                if progress
                else None
            )
            counters = None
            if source_progress is not None:
                counters = source_progress.counts
                if source_progress.coverage != "complete":
                    source_state = "degraded" if success else "unavailable"
                elif progress is not None:
                    source_state = (
                        "stale"
                        if (
                            clock
                            - (source_progress.last_successful_retrieval_at or progress.updated_at)
                        ).total_seconds()
                        > settings.detection_max_age_hours * 3600
                        else "ok"
                    )
            elif attempt and attempt[0].status == JobStatus.SUCCEEDED:
                if attempt[0].rejected_count:
                    source_state = "degraded"
            if (
                source_state == "ok"
                and acquisition is not None
                and (clock - acquisition).total_seconds() > settings.detection_max_age_hours * 3600
            ):
                source_state = "stale"
            sources.append(
                SourceStatus(
                    region_id=region.id,
                    product=product,
                    status=source_state,
                    refresh_id=progress.refresh_id if source_progress and progress else None,
                    coverage=source_progress.coverage if source_progress else None,
                    fetched_count=counters.fetched
                    if counters
                    else (
                        attempt[0].fetched_count
                        if attempt
                        and attempt[0].status == JobStatus.SUCCEEDED
                        and not source_progress
                        else None
                    ),
                    accepted_count=counters.accepted
                    if counters
                    else (
                        attempt[0].accepted_count
                        if attempt
                        and attempt[0].status == JobStatus.SUCCEEDED
                        and not source_progress
                        else None
                    ),
                    rejected_count=counters.rejected
                    if counters
                    else (
                        attempt[0].rejected_count
                        if attempt
                        and attempt[0].status == JobStatus.SUCCEEDED
                        and not source_progress
                        else None
                    ),
                    duplicate_count=counters.duplicate
                    if counters
                    else (
                        attempt[0].duplicate_count
                        if attempt
                        and attempt[0].status == JobStatus.SUCCEEDED
                        and not source_progress
                        else None
                    ),
                    completed_chunks=source_progress.completed_chunks if source_progress else None,
                    total_chunks=source_progress.total_chunks if source_progress else None,
                    next_retry_at=source_progress.retry_at if source_progress else None,
                    last_attempt_at=attempt[1] if attempt else None,
                    last_successful_retrieval_at=success,
                    latest_detection_at=acquisition,
                    latest_attempt_succeeded=(source_progress.coverage == "complete")
                    if source_progress
                    else (attempt[0].status == JobStatus.SUCCEEDED if attempt else None),
                )
            )
    weather_time = session.scalar(
        select(func.max(WeatherRecord.retrieved_at)).where(
            WeatherRecord.provider == Provider.OPEN_METEO, WeatherRecord.product == "forecast"
        )
    )
    baseline_count = session.scalar(select(func.count()).select_from(ClimateBaselineRecord)) or 0
    metadata = model_info(request, session)
    live_observation = session.scalar(
        select(func.max(ClusterSnapshotRecord.latest_detection_at))
        .join(LiveRunHead, LiveRunHead.run_id == ClusterSnapshotRecord.run_id)
        .where(ClusterSnapshotRecord.latest_detection_at <= clock)
    )
    return StatusView(
        evaluated_at=clock,
        sources=sources,
        refreshes=refreshes,
        scorer=metadata,
        firms_configured=settings.firms_map_key is not None,
        chain_enabled=settings.chain_enabled,
        weather_latest_retrieved_at=weather_time,
        climate_baseline_count=baseline_count,
        **operational_status(
            session,
            settings,
            model_metadata(session, settings, request.app.state.artifacts, mode=RunMode.LIVE),
            clock,
            live_observation,
        ),
    )
