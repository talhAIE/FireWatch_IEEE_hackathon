# FireWatch deployed API evaluation

Audited 25 October 2026, 13:15 PKT.

Service: https://firewatch-ieee-hackathon.onrender.com

## Verdict

The backend is online and serves saved historical results with valid operator authentication. It is **not fully ready**, and it is **not serving a live fire-monitoring feed**. The main deployment defects are unavailable scorer artifacts and missing source files for evidence verification. HTTP 200 from a read endpoint means saved data was returned, not that current model inference or ingestion succeeded.

## Scope and evidence

82 sequential HTTPS requests covered all 18 documented operations (15 GET and 3 POST). GET/OPTIONS requests were read-only; POST checks used no authentication or deliberately schema-invalid input so no valid job could be created. No database, deployment, source file, model, approval, or configuration was changed. No refresh, replay, training, anchor transaction, or worker job was executed.

The deployed OpenAPI paths and component schemas match the repository. All 78 JSON application responses checked conform to the corresponding local Pydantic response/error models. The one unexpected HTTP status was readiness 503. Twenty-five consistency checks passed; four source-artifact checks failed, one for each alert. This is a bounded functional audit, not a load test, penetration test, independent model-science validation, or uptime guarantee.

Observed round-trip latency from this workstation: minimum 290 ms, median 574 ms, maximum 2541 ms across the mixed 82 requests. These are single audit samples, not an SLA or cold-start measurement.

## Findings and recommended fixes

### 1. High: Scorer unavailable: application readiness fails

GET /health/ready returns 503 ARTIFACT_UNAVAILABLE. GET /api/v1/models/current returns artifact_ready=false and scoring_available=false for forest-independent-2025-v2. Saved scores are still readable; they do not demonstrate that new inference works. Confirm the deployed assets, MODEL_DIR, checksum/dependency contract, and build configuration.

### 2. High: Raw source evidence cannot be verified on the deployed instance

All four saved alert payload hashes match, but verify_artifacts=true reports unavailable with missing_file for the referenced source files. These are missing bytes at the configured deployed locations, not proof of tampering. Package the public assets, verify SOURCE_ARTIFACT_DIR, and rerun the audit.

### 3. Medium: Replay listing does not discover the saved replay runs

GET /api/v1/replays returns total=0 while GET /api/v1/runs?mode=replay returns four runs. The route filters on eligible replay_scenario metadata. Inspect the discovery metadata/query; use /runs?mode=replay for current run selection. Do not rewrite immutable evidence just to populate a dropdown.

### 4. Configuration: Business reads require a bearer token

All 13 business GET routes reject anonymous access with 401. This matches the production default and protects the API. If the research dashboard should be public, explicitly set PUBLIC_READS_ENABLED=true for reads and keep operator mutations authenticated. Never put the admin token in a public frontend.

### 5. Configuration: Tested browser origin is not allowed

An OPTIONS preflight for http://localhost:5173 with Authorization was rejected with 400. Same-origin Swagger works; configure CORS_ORIGINS with the actual approved frontend origin before browser integration. Other origins were not tested.

## Is the data live?

No. Four completed runs have mode=replay and none has mode=live. Their cutoffs are 6 May 2024, 25 July 2024, and 26 July 2024. The ML example's latest detection is 25 July 2024 at 21:42 UTC. The runs were processed in October 2026; processing/retrieval timestamps do not make the observations current.

Both demo-region and northern-california have enabled=false and latest_live_run_id=null. /status reports firms_configured=false, all four region/product source states as never_fetched, no successful live refresh, and weather_latest_retrieved_at=null. The climate database has 48 baseline rows. Historical weather fields can exist while the live-weather status remains empty.

Freshness flags such as observations_stale=false are evaluated relative to the replay cutoff (reference_kind=historical_cutoff), not today's time. They must not be displayed as proof of a current live feed.

The approved forest's recorded scope is historical northern-California replay. Live NRT/NOAA-21, Pakistan, or other regions need separate validation and approval. Enabling a NASA key alone does not grant that model approval.

## Inputs and processing

The current API has no generic /predict endpoint and no image/video upload route. Most endpoints retrieve saved runs; they do not accept arbitrary sensor measurements for immediate model scoring.

1. Source pipeline inputs: NASA FIRMS/VIIRS thermal observations with coordinates, acquisition time, satellite/product, confidence, brightness, and fire radiative power (FRP). Open-Meteo supplies weather context; NASA POWER supplies climate baseline context. Current hosted outputs are historical provider-derived records; their raw source bytes cannot be revalidated on this deployment until the missing files are restored.
2. Processing: normalize/deduplicate observations, group nearby observations into hotspot snapshots, calculate temporal/thermal/weather features, apply an eligible scorer, and store results and evidence in PostgreSQL.
3. API read inputs: run_id, region_id, snapshot_id or alert_id; priority and geographic bounding-box filters; pagination. A bbox uses west,south,east,north in decimal degrees. Normal pages default to 50 and allow at most 200; GeoJSON defaults to and caps at 2000, with pagination metadata.
4. Operator inputs: refresh takes region_id plus ordered UTC window_start/window_end; replay takes a configured scenario_id; anchor takes an existing alert_id and the server-selected chain configuration. All three require the server operator bearer token and a unique Idempotency-Key of 8–128 allowed characters. A 202 response queues work; a worker must execute it.

Features exposed in the sampled hotspot include detection counts over 6/12/24 hours; maximum and summed FRP in MW; maximum brightness in K; daytime fraction; first-seen age; temperature in C; humidity in percent; wind/gust in m/s; rainfall in mm; and temperature/rainfall departures with explicit missing flags and units. The delivered forest's actual non-leaf splits use nine satellite/activity features and do not use weather, climate, or missing-indicator splits. Displaying enriched features does not prove that this model uses those fields or that they improve its skill.

## Outputs and interpretation

The ML replay returns six hotspot snapshots: LOW=3, MEDIUM=2, HIGH=1, CRITICAL=0. The source data is marked partial. Baseline runs also return six snapshots with different category counts; score scales and categories must not be compared as calibrated probabilities across model versions.

One actual saved ML hotspot response, abbreviated:

```json
{
  "latitude": 39.79337504616922,
  "longitude": -121.80270004696976,
  "member_count": 2,
  "latest_detection_at": "2024-07-25T20:02:00Z",
  "frp_max_mw": 6.77,
  "frp_sum_mw": 13.54,
  "priority": "HIGH",
  "priority_score": 0.2433265211568792,
  "score_kind": "model_score",
  "escalation_probability": null,
  "scorer_version": "forest-independent-2025-v2"
}
```

The 0.2433265 value is an uncalibrated model ranking score; it does not mean a 24.33% chance of fire, escalation, or physical severity. escalation_probability is null and probability_supported=false. Priority thresholds are provisional. Explanations describe contributions to this model score, not causal drivers of a confirmed fire.

GeoJSON represents WGS84 Point coordinates in longitude,latitude order. Hotspot detail adds features, units, missing flags, quality flags, hashes, and explanations. Summary groups results by priority and score kind. Alerts provide saved evidence payloads and integrity statuses.

All four saved alert payloads match their saved SHA-256 digest. All four are unanchored, chain_enabled=false, and source-artifact verification is unavailable because files are missing. A matching saved digest is not independent blockchain notarization and does not establish that an observation is a real fire or a model is scientifically correct. The ML run currently has no linked alerts in its six returned hotspot items; the four sampled alerts belong to baseline runs.

## Complete endpoint reference

Every /api/v1 endpoint currently requires an operator bearer token on this deployment. Health and documentation endpoints are public. Authenticated query examples belong in Swagger's Authorize flow or a private API client; never embed the admin token in frontend code.

### GET /health/ready

- Input: None.
- Output: Database/schema/scorer readiness.
- Observed: 503 ARTIFACT_UNAVAILABLE.

### GET /api/v1/regions

- Input: limit, offset.
- Output: Region IDs, names, bounds, enabled modes, latest live run.
- Observed: 200; 2 disabled regions.

### GET /api/v1/runs

- Input: region_id, mode, status, limit, offset.
- Output: Saved runs, stages, scorer version, freshness.
- Observed: 200; 4 replay runs; 0 live.

### GET /api/v1/runs/{run_id}

- Input: run_id UUID path.
- Output: One saved run and stage/freshness metadata.
- Observed: 200 for existing runs; 404 unknown.

### GET /api/v1/hotspots

- Input: Required run_id; optional priority, bbox, limit, offset.
- Output: Ranked locations, FRP, detection counts, priority, score, explanations, alert ID.
- Observed: 200; ML run has 6 hotspots.

### GET /api/v1/hotspots.geojson

- Input: Required run_id; optional priority, bbox, limit, offset.
- Output: GeoJSON FeatureCollection; coordinates=[longitude,latitude].
- Observed: 200; identities and counts match list.

### GET /api/v1/hotspots/{snapshot_id}

- Input: snapshot_id UUID path; optional run_id.
- Output: Hotspot, features, units, missing flags, explanation, optional saved evidence.
- Observed: 200 for populated runs; 404 unknown.

### GET /api/v1/summary

- Input: Required run_id; optional priority, bbox.
- Output: Counts by priority and score kind, filters and freshness.
- Observed: 200; totals match list and map.

### GET /api/v1/models/current

- Input: None.
- Output: Selected model identity, readiness, score semantics, limitations.
- Observed: 200 HTTP, but model/scoring unavailable.

### GET /api/v1/replays

- Input: limit, offset.
- Output: Replay scenarios discovered from eligible saved-run metadata.
- Observed: 200; empty despite 4 replay runs.

### GET /api/v1/alerts/{alert_id}

- Input: alert_id UUID path; optional run_id, verify_artifacts.
- Output: Redacted saved evidence payload, hashes and verification status.
- Observed: 200; 4 existing alerts readable.

### GET /api/v1/alerts/{alert_id}/verify

- Input: alert_id UUID path; optional run_id, verify_artifacts=true.
- Output: Saved payload digest, optional source/model byte audit, anchor status.
- Observed: 200; hashes match, source files missing for all 4.

### GET /api/v1/status

- Input: None.
- Output: Source freshness, live refreshes, scorer, FIRMS/chain flags.
- Observed: 200; no live ingestion; model unavailable.

### GET /api/v1/jobs/{job_id}

- Input: job_id UUID path; operator bearer required.
- Output: Durable job state, timing, progress, result run or safe error.
- Observed: Unknown ID correctly 404; existing job not sampled.

### POST /api/v1/jobs/refresh

- Input: Bearer + Idempotency-Key; JSON region_id, window_start, window_end.
- Output: 202 job_id/status; queues refresh, does not finish it synchronously.
- Observed: 401 without token; 422 invalid body; execution not tested.

### POST /api/v1/jobs/replay

- Input: Bearer + Idempotency-Key; JSON scenario_id.
- Output: 202 job_id/status; queues configured replay.
- Observed: 401 without token; 422 invalid body; execution not tested.

### POST /api/v1/alerts/{alert_id}/anchor

- Input: Bearer + Idempotency-Key; alert_id UUID; empty JSON object.
- Output: 202 job acceptance for configured chain anchoring.
- Observed: 401 without token; 422 malformed ID; chain disabled.

### GET /health/live

- Input: None.
- Output: Process liveness: status=ok.
- Observed: 200; process alive.

## Error and browser behavior

Anonymous business GET requests and unauthenticated mutation requests return 401 with WWW-Authenticate: Bearer. An invalid bearer is rejected. Missing required run IDs, malformed UUIDs, invalid priority/bbox, limit=201, negative offset, and invalid mutation bodies return structured 422 errors. Unknown runs, hotspots, alerts, and jobs return structured 404 errors. These boundary behaviors are working.

/docs, /redoc, and /openapi.json return 200. The root URL / returns 404 because there is no homepage. Public availability of Swagger does not mean its protected business routes are anonymously readable.

## Fix order

1. Resolve model readiness: verify deployment branch/commit and runtime; include deploy/render/model and deploy/render/assets. For native Render Python, use the locked requirements build plus scripts/render_assets.py verify and MODEL_DIR=deploy/render/model, SOURCE_ARTIFACT_DIR=deploy/render/assets. For a Docker deployment, the generic root Dockerfile excludes operational assets; use an image that explicitly includes the prepared public deployment files, such as deploy/northflank.Dockerfile with port 8000. If files are present, verify their digests and the inference manifest's Python/numpy/pydantic contract. The API alone cannot distinguish missing model bytes from every possible model-contract failure.
2. Rerun /health/ready and all four verify_artifacts=true checks; require readiness 200 and artifact_verification=verified. The API's 200 status must be checked together with its semantic readiness fields.
3. Decide whether the dashboard should read public research data. If so, explicitly enable PUBLIC_READS_ENABLED=true and configure exact CORS_ORIGINS for that frontend. Keep operator routes authenticated and the admin token server-side.
4. Correct or account for the empty replay-discovery endpoint. Existing replay outputs can be selected through /runs?mode=replay.
5. If live ingestion is required, obtain/configure FIRMS_MAP_KEY, enable a supported region, supply writable source storage, configure and operate a worker/scheduler, and select a scorer that is validated and approved for live mode. Validate fresh acquisition timestamps and successful completed live runs. The current historical forest approval is insufficient. This audit did not enable live ingestion.

## Limits of this audit

Successful job enqueue/execution, retries, scheduling, fresh provider ingestion, and on-chain transactions were not exercised. Existing job success was not sampled because no valid existing job ID was obtained through the public contract; an authenticated unknown job correctly returned 404. No destructive or valid mutation request was submitted. No hosting settings were edited. Latency excludes Render's future cold starts and does not establish concurrent-load capacity.

Primary evidence is in the accompanying deployed-api-audit-2026-10-25.json: timestamped requests, response bodies, the deployed OpenAPI document, and consistency checks. Credentials and authorization headers were not saved.
