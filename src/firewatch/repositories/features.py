"""Freeze cached enrichment and persist immutable features-v1 inside one Run transaction."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

from pydantic import Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from firewatch.db.models import FeatureSetRecord, IngestionBatch, Run, SourceAsset, WeatherRecord
from firewatch.processing.clusters import SnapshotBatch
from firewatch.processing.features import (
    AssetPin,
    FeatureEnrichment,
    FeatureSpec,
    SatelliteAssetPin,
    build_features,
    validate_feature_set,
)
from firewatch.repositories.baselines import get_baseline
from firewatch.repositories.revisions import RevisionPolicy, freeze_firms_inputs
from firewatch.repositories.snapshots import build_run_snapshots
from firewatch.repositories.weather import cached_weather
from firewatch.schemas.common import DTO, RunStatus, Stage, StageStatus, UTCDateTime
from firewatch.schemas.contracts import (
    ClimateBaseline,
    ClusterSnapshot,
    FeatureSet,
    WeatherRecordInput,
    WeatherSeries,
)
from firewatch.sources.firms_normalize import digest
from firewatch.sources.open_meteo import (
    GRID,
    NORMALIZER,
    WeatherRequest,
    parse_weather,
    utc,
    weather_cell,
)
from firewatch.sources.power import climate_cell


class EnrichmentPins(DTO):
    retrieval_cutoff: UTCDateTime
    weather: dict[str, WeatherSeries] = Field(default_factory=dict)
    baselines: dict[str, ClimateBaseline | None] = Field(default_factory=dict)
    assets: list[AssetPin] = Field(default_factory=list)
    satellite_assets: list[SatelliteAssetPin] = Field(default_factory=list)


def _request(snapshot: ClusterSnapshot, config: FeatureSpec) -> WeatherRequest:
    product, model = config.weather_identity(snapshot.mode)
    end = snapshot.as_of.replace(minute=0, second=0, microsecond=0)
    return WeatherRequest.model_validate(
        dict(
            cells=[weather_cell(snapshot.latitude, snapshot.longitude)],
            start=end - timedelta(hours=23),
            end=end,
            product=product,
            model=model,
            source_version=config.weather_namespace,
            mode=snapshot.mode,
        )
    )


def _weather_slice(session: Session, request: WeatherRequest, cutoff: datetime) -> WeatherSeries:
    full = cached_weather(session, request, cutoff)
    if full is not None:
        return full
    # Keep one partial revision rather than treating available current weather as missing.
    rows = list(
        session.scalars(
            select(WeatherRecord)
            .join(SourceAsset, SourceAsset.id == WeatherRecord.asset_id)
            .where(
                WeatherRecord.cell_key == request.cells[0].key,
                WeatherRecord.provider == "open_meteo",
                WeatherRecord.product == request.product,
                WeatherRecord.model == request.model,
                WeatherRecord.valid_at >= request.start,
                WeatherRecord.valid_at <= request.end,
                WeatherRecord.retrieved_at <= cutoff,
                SourceAsset.processing_version == NORMALIZER,
                SourceAsset.request_metadata["grid_version"].astext == GRID,
                SourceAsset.request_metadata["cache_namespace"].astext == request.source_version,
            )
            .order_by(WeatherRecord.retrieved_at.desc(), WeatherRecord.source_version.desc())
            .limit(100001)
        )
    )
    if len(rows) > 100000:
        raise ValueError("Feature weather revision bound exceeded")
    if not rows:
        return WeatherSeries(records=[], quality_flags=["weather_cache_missing"])
    revision = rows[0].source_version
    chosen = [WeatherRecordInput.model_validate(r) for r in rows if r.source_version == revision]
    return WeatherSeries(
        records=sorted(chosen, key=lambda r: r.valid_at),
        quality_flags=["weather_cache_partial_interval"],
    )


def collect_enrichment(
    session: Session,
    run: Run,
    snapshots: SnapshotBatch,
    policy: RevisionPolicy,
    config: FeatureSpec,
    cutoff: datetime,
) -> EnrichmentPins:
    pins = EnrichmentPins(retrieval_cutoff=cutoff)
    batch_assets = session.execute(
        select(IngestionBatch.id, SourceAsset)
        .join(SourceAsset, SourceAsset.id == IngestionBatch.asset_id)
        .where(IngestionBatch.id.in_(policy.batch_ids))
    ).all()
    if len(batch_assets) != len(policy.batch_ids):
        raise ValueError("Feature satellite lineage is unavailable")
    for batch_id, asset in batch_assets:
        if asset.retrieved_at > cutoff:
            raise ValueError("Pinned satellite source is newer than feature availability cutoff")
        pins.satellite_assets.append(
            SatelliteAssetPin(batch_id=batch_id, asset_id=asset.id, sha256=asset.content_hash)
        )
    assets: set[UUID] = set()
    for details in snapshots.snapshots:
        request = _request(details.snapshot, config)
        cell = request.cells[0]
        if cell.key not in pins.weather:
            if len(pins.weather) >= config.max_weather_cells:
                raise ValueError("Feature weather cell bound exceeded")
            pins.weather[cell.key] = _weather_slice(session, request, cutoff)
            assets.update(r.asset_id for r in pins.weather[cell.key].records)
        climate = climate_cell(cell.latitude, cell.longitude)
        if climate.key not in pins.baselines:
            baseline = get_baseline(
                session, climate, run.as_of.month, config.climate, retrieval_cutoff=cutoff
            )
            pins.baselines[climate.key] = baseline
            if baseline:
                assets.add(baseline.asset_id)
    for identifier in sorted(assets, key=lambda i: i.hex):
        asset = session.get(SourceAsset, identifier)
        if asset is None or asset.retrieved_at > cutoff:
            raise ValueError("Feature enrichment source is unavailable at cutoff")
        pins.assets.append(AssetPin(asset_id=identifier, sha256=asset.content_hash))
    pins.satellite_assets.sort(key=lambda p: p.batch_id.hex)
    return pins


def _verify_pins(
    session: Session,
    pins: EnrichmentPins,
    config: FeatureSpec,
    month: int,
    source_contents: dict[UUID, bytes] | None = None,
) -> None:
    # Hashes bind source identity; normalized weather rows are checked against raw JSON too.
    total_bytes = 0
    for satellite in pins.satellite_assets:
        batch = session.get(IngestionBatch, satellite.batch_id)
        if batch is None or batch.asset_id != satellite.asset_id:
            raise ValueError("Pinned feature batch/source association changed")
    for pin in pins.assets + pins.satellite_assets:
        asset = session.get(SourceAsset, pin.asset_id)
        if (
            asset is None
            or asset.content_hash != pin.sha256
            or asset.retrieved_at > pins.retrieval_cutoff
        ):
            raise ValueError("Pinned feature source identity or availability changed")
    weather_assets = {r.asset_id for series in pins.weather.values() for r in series.records}
    for identifier in weather_assets:
        asset = session.get(SourceAsset, identifier)
        if (
            asset is None
            or asset.provider != "open_meteo"
            or asset.processing_version != NORMALIZER
        ):
            raise ValueError("Invalid pinned weather source")
        if source_contents is not None:
            if identifier not in source_contents:
                raise ValueError("Pinned weather bytes were not prepared")
            raw = source_contents[identifier]
        else:
            path = Path(asset.storage_path)
            with path.open("rb") as source:
                raw = source.read(8 * 1024 * 1024 + 1)
        total_bytes += len(raw)
        if total_bytes > 32 * 1024 * 1024:
            raise ValueError("Feature weather evidence size bound exceeded")
        if len(raw) > 8 * 1024 * 1024 or hashlib.sha256(raw).hexdigest() != asset.content_hash:
            raise ValueError("Pinned weather evidence checksum mismatch")
        request = WeatherRequest.model_validate(asset.request_metadata["request"])
        parsed = parse_weather(
            json.loads(raw),
            request,
            asset.retrieved_at,
            asset.id,
            f"{request.source_version}-{asset.id.hex}",
        )
        expected = {(r.cell_key, r.valid_at, r.source_version): r for r in parsed.records}
        stored = {
            (r.cell_key, r.valid_at, r.source_version): WeatherRecordInput.model_validate(r)
            for r in session.scalars(
                select(WeatherRecord).where(WeatherRecord.asset_id == identifier)
            )
        }
        for series in pins.weather.values():
            for row in series.records:
                if row.asset_id != identifier:
                    continue
                key = (row.cell_key, row.valid_at, row.source_version)
                if expected.get(key) != row or stored.get(key) != row:
                    raise ValueError(
                        "Pinned normalized weather changed or disagrees with raw source"
                    )
    for climate_key, baseline in pins.baselines.items():
        if baseline is None:
            continue  # Frozen missing context never silently fills on retry.
        current = get_baseline(
            session,
            climate_cell(baseline.latitude, baseline.longitude),
            month,
            config.climate,
            retrieval_cutoff=pins.retrieval_cutoff,
        )
        if current != baseline or current.cell_key != climate_key:
            raise ValueError("Pinned climate baseline changed")


def _enrichments(snapshots: SnapshotBatch, pins: EnrichmentPins) -> list[FeatureEnrichment]:
    assets = {p.asset_id: p for p in pins.assets}
    result = []
    for details in snapshots.snapshots:
        snapshot = details.snapshot
        cell = weather_cell(snapshot.latitude, snapshot.longitude)
        climate = climate_cell(cell.latitude, cell.longitude)
        series = pins.weather[cell.key]
        baseline = pins.baselines[climate.key]
        result.append(
            FeatureEnrichment(
                snapshot_id=snapshot.snapshot_id,
                weather=series,
                weather_assets=[
                    assets[i] for i in sorted({r.asset_id for r in series.records}, key=str)
                ],
                baseline=baseline,
                baseline_asset=assets[baseline.asset_id] if baseline else None,
            )
        )
    return result


def build_run_features(
    session: Session,
    run_id: UUID,
    config: FeatureSpec | None = None,
    *,
    retrieval_cutoff: datetime | None = None,
    enrichment_pins: EnrichmentPins | None = None,
    source_contents: dict[UUID, bytes] | None = None,
) -> list[FeatureSet]:
    """Caller owns transaction; cached inputs only. A retry never reselects enrichment."""
    if not session.in_transaction():
        raise ValueError("Use a caller-owned feature transaction")
    config = FeatureSpec.model_validate((config or FeatureSpec()).model_dump())
    run = session.scalar(select(Run).where(Run.id == run_id).with_for_update())
    if run is None or run.feature_version != config.feature_version:
        raise ValueError("Unknown Run or incompatible feature version")
    clustering = run.input_asset_manifest.get("clustering")
    firms = run.input_asset_manifest.get("firms")
    if not isinstance(clustering, dict) or not isinstance(firms, dict):
        raise ValueError("Complete the snapshot stage before building features")
    saved_snapshot = SnapshotBatch.model_validate(clustering["batch"])
    policy = RevisionPolicy.model_validate(firms["policy"])
    snapshots = build_run_snapshots(session, run_id, policy, saved_snapshot.config)
    selected = freeze_firms_inputs(session, run_id, run.as_of - timedelta(hours=24), policy)
    existing = run.input_asset_manifest.get("features")
    if existing is not None:
        body = {k: v for k, v in existing.items() if k != "sha256"}
        if digest(body) != existing["sha256"] or body["config"] != config.model_dump(mode="json"):
            raise ValueError("Saved feature stage or configuration changed")
        pins = EnrichmentPins.model_validate(body["pins"])
        if enrichment_pins is not None and pins != enrichment_pins:
            raise ValueError("Replay enrichment pins cannot change on retry")
        if retrieval_cutoff is not None and utc(retrieval_cutoff) != pins.retrieval_cutoff:
            raise ValueError("Feature availability cannot be repinned")
    else:
        if run.status != RunStatus.RUNNING:
            raise ValueError("Only running Runs can create features")
        cutoff = utc(retrieval_cutoff or datetime.now(UTC))
        pins = (
            EnrichmentPins.model_validate(enrichment_pins.model_dump())
            if enrichment_pins is not None
            else collect_enrichment(session, run, snapshots, policy, config, cutoff)
        )
        if retrieval_cutoff is not None and pins.retrieval_cutoff != cutoff:
            raise ValueError("Replay availability cutoff differs from pinned inputs")
    _verify_pins(session, pins, config, run.as_of.month, source_contents)
    computed = build_features(
        snapshots, selected, _enrichments(snapshots, pins), pins.satellite_assets, config
    )
    stored = list(
        session.scalars(select(FeatureSetRecord).where(FeatureSetRecord.run_id == run_id))
    )
    expected = {f.snapshot_id: f for f in computed}
    if existing is not None:
        if (
            body["snapshot_sha256"] != clustering["sha256"]
            or body["feature_hashes"] != {str(f.snapshot_id): f.feature_hash for f in computed}
            or {r.snapshot_id for r in stored} != set(expected)
        ):
            raise ValueError("Saved feature references or inputs changed")
        for row in stored:
            actual = FeatureSet(
                run_id=run_id,
                mode=run.mode,
                as_of=run.as_of,
                snapshot_id=row.snapshot_id,
                feature_version=row.feature_version,
                values=row.values,
                missing_flags=row.missing_flags,
                units=row.units,
                lineage=row.lineage,
                feature_hash=row.feature_hash,
            )
            if validate_feature_set(actual) != expected[row.snapshot_id]:
                raise ValueError("Saved feature row differs from frozen computation")
        return computed
    if stored:
        raise ValueError("Run has feature rows without a stage manifest")
    session.add_all(
        [
            FeatureSetRecord(
                snapshot_id=f.snapshot_id,
                run_id=run_id,
                feature_version=f.feature_version,
                values=f.values,
                missing_flags=f.missing_flags,
                units=f.units,
                lineage=f.lineage,
                feature_hash=f.feature_hash,
            )
            for f in computed
        ]
    )
    body = {
        "schema_version": "feature-stage-v1",
        "config": config.model_dump(mode="json"),
        "pins": pins.model_dump(mode="json"),
        "snapshot_sha256": clustering["sha256"],
        "feature_hashes": {str(f.snapshot_id): f.feature_hash for f in computed},
    }
    run.input_asset_manifest = dict(run.input_asset_manifest) | {
        "features": body | {"sha256": digest(body)}
    }
    run.stage_status = dict(run.stage_status) | {
        Stage.ENRICHMENT.value: StageStatus.SUCCEEDED.value,
        Stage.FEATURES.value: StageStatus.SUCCEEDED.value,
    }
    session.flush()
    return computed


def fork_feature_run(session: Session, source_run_id: UUID, config: FeatureSpec) -> UUID:
    """Explicit new Run for reevaluating enrichment with the same frozen satellite inputs."""
    from copy import deepcopy

    if not session.in_transaction():
        raise ValueError("Use a caller-owned feature fork transaction")
    source = session.get(Run, source_run_id)
    if source is None or source.feature_version != config.feature_version:
        raise ValueError("Unknown source Run or incompatible feature schema")
    clustering = source.input_asset_manifest.get("clustering")
    firms = source.input_asset_manifest.get("firms")
    if not isinstance(clustering, dict) or not isinstance(firms, dict):
        raise ValueError("A feature fork requires saved snapshot inputs")
    batch = SnapshotBatch.model_validate(clustering["batch"])
    policy = RevisionPolicy.model_validate(firms["policy"])
    build_run_snapshots(session, source_run_id, policy, batch.config)
    identifier = uuid4()
    session.add(
        Run(
            id=identifier,
            region_id=source.region_id,
            mode=source.mode,
            as_of=source.as_of,
            status=RunStatus.RUNNING,
            config_hash=digest(config.model_dump(mode="json")),
            run_key="feature-fork:" + identifier.hex,
            completeness=source.completeness,
            stage_status={},
            input_asset_manifest={"firms": deepcopy(firms)},
            scorer_version=source.scorer_version,
            feature_version=config.feature_version,
            started_at=datetime.now(UTC),
        )
    )
    session.flush()
    build_run_snapshots(session, identifier, policy, batch.config)
    return identifier
