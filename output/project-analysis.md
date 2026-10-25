# FireWatch AI — project analysis

Analyzed 24 October 2026, branch `devtalha`, commit `b8963f8`. This is a review of the checked-out source, delivered artifacts, configuration, tests and recorded results. Recorded activity on the original machine is distinguished from checks performed on this machine. No application source, model, database or approval was changed.

## Overall assessment

FireWatch AI is a substantial Python backend for historical satellite-hotspot prioritization. It implements ingestion, enrichment, deterministic processing, model training and evaluation, a durable worker, typed APIs, evidence verification and optional blockchain anchoring. It is suitable as a research backend and a foundation for frontend integration. The delivered checkout needs portability and release-packaging corrections before its historical demonstration can be reproduced reliably on a fresh machine. It does not include a frontend, and this analysis does not establish live-fire or production readiness.

The source architecture is stronger than its delivery consistency. Particularly good decisions include immutable completed runs, database-enforced output integrity, separate API and worker processes, explicit missing-data handling, run-scoped frontend reads, non-executable model artifacts, event-disjoint evaluation and narrowly scoped model approvals.

## Repository and technology

- 12,763 tracked files; 1,479,434,592 bytes in this Windows working tree, approximately 1.48 GB before Git history and the installed environment.
- `data/`: 11,451 files, approximately 1.37 GB. `models/`: 793 files, approximately 94 MB. These dominate the delivery.
- `src/`: 106 Python files and 22,957 lines. Tests: 90 Python files and 16,060 lines. Scripts: 32 Python files and 4,456 lines. Line counts include comments and blank lines.
- Python 3.12 is explicitly required; FastAPI, Pydantic 2, SQLAlchemy 2, psycopg, Alembic, httpx, Typer, NumPy, PyArrow and scikit-learn form the main stack.
- Dependencies are hash-pinned: 71 runtime packages, 88 development packages and 29 separately locked security-tool packages.
- Three Alembic migrations, 21 ORM tables, 18 OpenAPI operations: 15 GET and three POST.
- There is no `package.json`, React/Vue application or HTML frontend in the tracked project. Swagger is the provided interactive API interface.

## Architecture and data flow

1. **Source acquisition:** `sources/firms.py` fetches bounded NASA FIRMS CSV responses. Persistent SQLite quota reservations, provider-observation reconciliation, retries, response limits and immutable raw manifests protect ingestion. Archive import supports validation, provenance, batching and resume.
2. **Normalization and revision selection:** observations are normalized and mapped to physical identities. Source revisions and product policies are selected explicitly and frozen for a run rather than silently replacing historical inputs.
3. **Enrichment:** Open-Meteo supplies hourly weather; NASA POWER supplies fixed-reference climate context. Separate source adapters and PostgreSQL cache repositories preserve retrieval identity, product, cutoff and missingness. Replay is designed to run from pinned cached artifacts.
4. **Snapshots:** deterministic haversine DBSCAN uses a 2 km neighborhood, six-hour candidate observations and a 24-hour history. Spherical centroids and independent 6/12/24-hour activity counts become immutable snapshots. Explicit input and neighborhood bounds control memory.
5. **Features:** the shared `features-v1` builder produces 17 values and 14 missing flags, giving 31 model inputs. Features cover FRP, brightness, activity, confidence, day/night, weather and climate departures. Null measurements retain units, flags and provenance.
6. **Scoring:** the maximum-FRP baseline and weighted baseline need no trained artifact. Approved supervised scoring uses a checksum-verified portable JSON forest, fixed preprocessing and thresholds, and exact tree-path-dependent SHAP explanations. Missing required FRP withholds ML scoring; failure does not silently substitute a baseline.
7. **Publication:** the pipeline writes snapshots, features, scores and evidence through recoverable stages. Publication checks required outputs and stage success. Completed runs and outputs are guarded against modification at the database layer. A live head advances transactionally by region and cutoff; replay remains separate.
8. **Serving:** API routes expose immutable run-scoped list/detail/summary/GeoJSON projections. This lets a frontend keep map, ranking and counts aligned by selecting one completed `run_id`.
9. **Optional anchoring:** evidence and the anchor outbox are written atomically. A separate signer records transaction identity before broadcast, handles retries, confirmations and reorgs. Solidity stores append-only SHA-256 commitments and rejects conflicting overwrites.

Core entry points: `src/firewatch/main.py`, `cli.py`, `worker.py`, `pipeline/runner.py`, `processing/features.py`, `ml/predict.py`, `evidence/anchor_delivery.py` and `db/models.py`.

## API and frontend integration

The generated OpenAPI matches both `artifacts/openapi.json` and `tests/fixtures/openapi-v1.json` exactly in this checkout.

- Health: `/health/live` and `/health/ready`. Liveness is independent of database/provider I/O; readiness checks schema revision and scorer availability.
- Discovery/history: `/api/v1/regions`, `/runs`, `/runs/{run_id}`, `/replays`, `/status` and `/models/current`.
- Results: `/hotspots`, `/hotspots.geojson`, `/hotspots/{snapshot_id}` and `/summary`.
- Evidence: `/alerts/{alert_id}` and `/alerts/{alert_id}/verify`, including opt-in source/model artifact checks.
- Operator reads: `/jobs/{job_id}` always requires bearer authentication.
- Operator writes: `POST /jobs/refresh`, `POST /jobs/replay` and `POST /alerts/{alert_id}/anchor`. These require bearer authentication and an idempotency key and enqueue durable work.

Business reads can be public in development. Production settings default private when the flag is unspecified, but Compose explicitly selects development settings and public reads by default. Deployment policy must therefore be chosen explicitly. This is a single operator-token design, without end-user login, per-user authorization or multi-tenant access control. A frontend that needs protected operations requires an appropriate server-side authentication boundary; the operator token must not be shipped to the browser.

Pagination, typed filters, explicit GeoJSON truncation, stable operation IDs, CORS allowlists, missing flags, research notices and historical freshness semantics are already provided. GeoJSON uses longitude/latitude order. The recorded replay is `partial`; its coverage warning belongs in the UI even when all displayed weather fields are filled.

## Durable processing and database

PostgreSQL is a deliberate dependency; SQLite is used only for local quota/final-test ledgers. ORM groups cover regions, source assets/imports, observations and memberships, weather/climate, jobs/runs, cluster snapshots/features, model registry/activation/promotion, scores, alerts, outbox and anchors.

The worker runs separately from ASGI, schedules bounded UTC slots, claims queued or expired jobs, heartbeats leases and fences writes by attempt number. Database transactions own admission, stage checkpoints and publication; provider I/O and scoring are outside long database transactions. Queue capacity, retry attempts, timeouts and stage recovery are configurable. The optional anchor signer has separate admission and signer serialization.

Migration `0002` adds database triggers for evidence immutability, completion/publication and output protection. Migration `0003` adds model registry governance. These protections are stronger than application-only checks, though application database role ownership and migration access still need an operational privilege policy for a deployed service.

## Model and evaluation findings

The recorded current release is `forest-independent-2025-v2`. Its managed review names Talha and limits approval to historical northern-California replay. The readiness receipt says the original database activated it. This fresh checkout has no configured serving database, so activation here has not been established.

The runtime artifact has 64 trees, maximum depth 8, minimum leaf size 1 and balanced class weights. Calibration status is `cv_uncalibrated`; `probability_supported=false`. A score must not be rendered as a calibrated percentage chance of escalation.

**Target:** later satellite-observed FRP strengthening within 24 hours and 2 km, exceeding current maximum FRP by more than `max(5 MW, 20% of current maximum)`. It measures a satellite observation proxy. It does not label confirmed ignitions, burned area, physical fire severity or actual emergency incidents.

The saved 2025 final holdout has 811 rows: 47 positive and 764 negative, across 438 thermal partition groups. Average precision is:

- Forest: **0.109701**.
- Maximum-FRP baseline: **0.050746**.
- Weighted baseline: **0.055594**.
- Logistic comparison: **0.139349**.
- Forest without climate: **0.108541**.
- Forest without weather/climate: **0.111370**.

The recorded paired whole-event 95% AP-improvement intervals are positive against both baselines: `[0.020698, 0.130192]` and `[0.008255, 0.124696]`, with 200 valid replicates each. These support the project's baseline-comparison gate. They do not establish that the forest beats logistic regression or that its raw score is calibrated.

At the saved forest High threshold, precision is **9.45%** and recall **51.06%**: 24 true positives, 230 false positives, 23 false negatives and 534 true negatives. This is useful context for an analyst workload discussion. Average precision is a ranking metric, not overall accuracy.

**Weather/climate model influence:** direct inspection of every non-leaf node in the delivered 64-tree forest found splits on nine satellite/activity features only. No weather, climate or missing-flag feature is used by those trees. Displaying real weather on the six demonstration hotspots therefore does not establish weather-sensitive predictions from this release. The saved ablation conclusions also mark additional weather/climate skill as not established.

**Evaluation governance:** source implements temporal/event separation, 48-hour purges, training-only preprocessing, development-only selection, independently audited calibration and a single-use final-test ledger. The delivered read-only ledger contains one completed claim and one `blocked_missing_original_ledger` entry for the previously consumed 2024 period. That is a recovery block, not a reconstructed original 2024 evaluation history. No final evaluation or retraining was run during this analysis.

## Evidence, chain and security posture

Strengths verified in source:

- Constant-time bearer comparison; duplicate authorization-header rejection.
- Authentication before parsing operator bodies; actual streamed-byte limits and body timeout.
- Duplicate JSON-member and non-finite-number rejection, explicit JSON-only mutation bodies and required idempotency keys.
- Read-only SQL transactions for public read endpoints, bounded connection pools and statement timeouts.
- Explicit CORS origins, safe error envelopes and request IDs, secret redaction and sanitized public projections.
- Bounded model/package reads, directory containment, symlink checks and checksum verification. Runtime model loading uses JSON rather than pickle/joblib.
- Canonical evidence bytes are preserved and hashed. Local payload status, artifact availability/integrity and chain status remain separate, avoiding claims that an RPC outage proves tampering.
- Solidity restricts writes to one immutable writer, rejects zero identities/digests, permits identical retries and prevents conflicting replacements.

Operational limitations: operator identity is shared-token based; no end-user permissions or API rate-limiting middleware was found in the inspected application. A hash proves byte integrity relative to a trusted digest, not the truth of a fire or the authenticity of a human review. A local Anvil deployment supplies no independent public notarization. This was a source-level security review with tests, not a penetration test or a fresh external vulnerability audit.

## Deployment and operations

Docker builds a fresh wheel from source, installs hashed dependencies and runs as UID 10001. Compose separates database, migrations, seed, API, pipeline worker and operator tools; chain and Caddy are optional profiles. Persistent volumes, loopback published ports, a private database network, read-only API artifact mounts, read-only root filesystems, dropped capabilities and log rotation are useful foundations.

The included Caddy configuration is local HTTP. Documentation explicitly leaves public hosting, production HTTPS, monitoring and backup/restore acceptance unfinished. A consistent database dump is delivered; its recorded artifact paths reflect the original machine and need a reviewed relocation/restore procedure.

The native `scripts/native_postgres.py` helper can provision a separate Windows installation outside OneDrive, but the newer historical-readiness guide calls a different `.local` script with missing prerequisites. This analysis did not install/start PostgreSQL, restore the dump, launch Docker, call providers, run Solidity tooling or submit chain transactions.

## Prioritized findings

### P1 — Distributed packages omit current evaluation and ingestion changes

Both `dist/firewatch_ai-0.1.0-py3-none-any.whl` and `dist/firewatch_ai-0.1.0.tar.gz` differ from current source in five modules: `ml/labels.py`, `ml/preprocessing.py`, `ml/split_package.py`, `ml/splits.py` and `sources/firms.py`. Comparisons normalized CRLF/LF before identifying differences.

The old wheel's split implementation lacks the current guard excluding target/event support beyond the development boundary for a later gapped holdout. It also lacks the current 2025 dataset design and newer FIRMS quota reconciliation. Installing these delivered archives can behave differently from editable installation and cannot be treated as the analyzed source release.

Action: rebuild both distributions from the reviewed commit, update build metadata and inventory, and run the independent-2025 design and quota regressions against an installed wheel. Docker's source build avoids this specific prebuilt-wheel problem.

### P1 — Windows long paths break Python artifact verification in this workspace

All 760 affected model evidence files exist and match their recorded SHA-256 when accessed with Windows extended paths. Normal `Path.is_file()` cannot access them on this machine. Git's `core.longpaths=true` fixes Git checkout but does not fix Python filesystem access.

The compact runtime inference bundle passes verification. Full `verify_managed_release(...)` fails with `ValueError: Release evidence must be a regular file`. `scripts/check_scaffold.py` consequently reports these inaccessible reviewed artifacts as forbidden generated files. This is a local path-access failure, not proof of corrupt or absent model bytes.

Action: shorten the workspace/release paths or enable supported OS/runtime long-path handling; verify the complete managed release and scaffold in the intended installation path. Do not delete or rewrite pinned artifacts to silence the error.

### P1 — Historical-readiness startup is not portable to this fresh clone

`docs/local-real-readiness.md:51` calls `.local/model-training/prepare_training_database.py`. That script expects `.local/phase52/pgsql/bin` at line 14, but the runtime is excluded from delivery. Its database-state JSON and `.env` are absent as expected for secrets. Creating a new database and applying migrations also does not restore the historical dump or activate the recorded model.

`.local/model-training/restart_ready_services.ps1:2` hardcodes `C:/Users/talha/Desktop/FireWatch_AI`. The current workspace belongs to PC under OneDrive.

Action: document and verify one fresh-clone path that provisions credentials/runtime, migrates or restores into a new target, installs/rebases managed artifacts, configures the scorer/replay catalog and proves API/worker readiness. Derive service paths from the script location.

### P2 — Delivery inventory is stale and not uniformly checkout-stable

Of 12,761 inventory entries, all files exist and all 760 long-path entries match using extended-path access. For normally accessible files, 23 working-tree hashes differ. Thirteen differences are explained by checkout line endings and match committed Git bytes. Ten inventory entries also disagree with the committed file bytes: `.gitattributes`, `.github/workflows/ci.yml`, `.local/model-training/server.log`, five JSON documents/receipts, `scripts/check_scaffold.py` and `tests/fixtures/openapi-v1.json`.

Action: regenerate the inventory after finalizing release contents. Define whether hashes bind Git blobs or checkout bytes and give every exact-byte artifact appropriate attributes. Distinguish archival snapshots from current evidence. The overall inventory does not currently certify the whole branch as delivered.

### P2 — Current and historical documentation conflict

README still says models are documentation-only and that no ML artifact is approved, whereas current review/readiness records describe an approved historical forest. The model card begins with activation, then includes a recovery paragraph saying neither candidate is approved, and retains older metrics/status statements. Some historical sections are labeled, but the recovery paragraph remains easy to misread. Current setup guidance also assumes services and files that are not present on a new host.

Action: create a concise authoritative current-status section, label old receipts/cards as historical, and separate the clean-install procedure from original-machine recovery instructions.

### Product boundary — Historical ranking is implemented; broader fire prediction is unproven

The model uses satellite/activity splits only, has weak absolute precision at its selected boundary, loses numerically to the logistic comparison and is approved only for a specific historical region/product/mode. The sample region for Pakistan remains disabled in configuration. These are substantive scope limits rather than coding defects.

Action: keep displayed score semantics and scope explicit. Use a newly reserved prospective cohort for future live/regional validation and weather-enriched development. Do not select a replacement using the already consumed final holdout.

### P3 — Operational health and read-query scaling can improve

`runtime.py:47` worker health checks database/schema and directory permissions. It does not prove queue progress or recent worker heartbeat. A stalled job loop can coexist with a healthy dependency probe. `run_view()` calls `run_times()` for each run, producing repeated aggregate queries as history pages grow.

Action: add queue-age/progress/heartbeat monitoring and aggregate run timestamps in bulk if measured history-read latency warrants it. These are source-level observations; no load test was performed.

## Validation and evidence boundaries

Fresh local checks: hash-locked environment installation, editable package installation, dependency consistency, Ruff (passed), formatting (238 files passed), strict mypy (106 source files passed), CLI help, OpenAPI equivalence (both saved contracts match), runtime bundle loading (passed), full-release verification attempt (failed on long-path access), delivery hash comparisons, package/source comparisons and offline pytest. Pytest finished with **932 passed, 342 skipped, zero failures**, in 60.92 seconds. The skipped cases require a disposable PostgreSQL database. One dependency deprecation warning concerns Starlette TestClient's use of httpx. Detailed results are in `output/project-analysis-tests.xml`.

The initial sandboxed test process stalled at a FastAPI TestClient test. The stalled runs were stopped; the successful offline run used the approved execution outside the sandbox with a separate test temporary directory. This is an execution-environment limitation rather than a demonstrated application failure.

The original committed `artifacts/readiness-regression.xml` records 1,274 collected cases, zero failures/errors and one skip, corresponding to the documented 1,273 passed. That is historical evidence from the original host. This clone has no configured disposable PostgreSQL database, so database-dependent integration/contract tests are skipped locally. Fresh testing must not be presented as a reproduction of the original full database-backed run.

No provider coverage was re-probed; no production deployment, backup restore, live model generalization or public-chain behavior was validated. The recorded 19,771 observations, 288 weather rows, 48 climate references and historical publication state come from delivered receipts/examples, not a freshly restored database query.

## Suggested order of work

1. Resolve local path access, stale distributions and fresh-clone startup/restore reproducibility.
2. Reconcile current documentation and regenerate final delivery/build evidence.
3. Run the complete suite against a new disposable PostgreSQL database and verify a restored historical replay with API/worker consistency and artifact audits.
4. Integrate a frontend using completed run IDs, historical timestamps, explicit score types, missing data and completeness warnings.
5. Evaluate monitoring, backups/restore, HTTPS and authenticated operations for the intended deployment.
6. Treat live NRT, new regions and weather-sensitive predictive skill as separate model-validation projects using new held-out evidence.
