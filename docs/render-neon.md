# Render Free API with an existing Neon database

This deployment runs one historical research API. Neon remains the database.
The Free web service uses one Uvicorn process and a small database pool. There
is no background worker, paid disk, new database, live ingestion, or retraining.
Operator jobs require a separate worker; do not submit them on this deployment.

`render.yaml` pins the service to `devtalha`, native Python 3.12 (through
`.python-version`), the Free plan, and a liveness health check. Liveness avoids
periodic health checks waking the Neon database. Check `/health/ready` after
deployments to verify the database, revision 0003, and approved model.

## Deployment assets

`deploy/render/manifest.json` pins 49 unique public source files covering 607
database source records and the approved JSON inference bundle, totaling about
3.4 MiB. These files are deliberate public deployment inputs; credentials,
database dumps, training packages, and workstation configuration are excluded.
Git stores the pinned artifact bytes without line-ending conversion.

`SOURCE_ARTIFACT_DIR` lets evidence verification locate source bytes by checksum
on either Windows or Linux. It still requires the original database record and
matching checksum and checks the file bytes within the configured directory.
The saved source locators and canonical evidence in Neon are unchanged.

Regenerate deliberately from a configured, trusted checkout:

```powershell
.\.venv\Scripts\python.exe scripts/render_assets.py prepare
.\.venv\Scripts\python.exe scripts/verify_render.py
```

Preparation reads the configured database and verified source files. Review and
scan newly prepared public artifacts before committing them. Existing differing
files are rejected. Render's build runs `scripts/render_assets.py verify` without
connecting to Neon or reading secrets.

## Create the Free web service

Connect `IbtisamAsghar/FireWatch_AI` on branch `devtalha`. Use the Blueprint or
copy the exact build/start commands and environment settings from `render.yaml`.
Choose **Free**, native **Python 3**, and the **Ohio** region (near this Neon
project). Do not choose Docker: the generic image intentionally excludes the
private operational data/model trees and has a different deployment contract.

Set `DATABASE_URL` in Render to the private direct Neon URL, with the scheme
`postgresql+psycopg://` and its original SSL query parameters. Never paste it into
Git, build commands, logs, screenshots, or documentation. Generate a separate
URL-safe `ADMIN_API_TOKEN` of at least 32 characters; keep it private.
The Blueprint generates this token automatically. The local `.env` and
`.env.neon` remain ignored.

The database is already restored. Do not seed, restore, or run migrations during
this deployment. There is no pre-deploy command or automatic migration.

After deployment, open `/docs` and `/health/ready`, then run:

```powershell
.\.venv\Scripts\python.exe scripts/verify_render.py --base-url https://YOUR-SERVICE.onrender.com
```

The read-only checks verify model metadata, six historical hotspots, and all
four alert evidence records against the deployed portable files.

## Repairing ARTIFACT_UNAVAILABLE

The root Docker image now includes `deploy/render` and copies the reviewed model
to `/var/lib/firewatch/models` as well as `/app/deploy/render/model`. Earlier
images omitted these public files, so a database with an approved model could
start successfully yet return 503 from `/health/ready`. The build now runs
`python scripts/render_assets.py verify --runtime`: it checks the file digests
and actually loads the inference bundle using the locked runtime dependencies.
The readiness route remains strict; it does not hide missing models or silently
switch to the baseline.

For an existing **Docker** Render service, pull the latest `devtalha` commit and
select **Manual Deploy → Deploy latest commit** (clear the build cache if the
old artifact-free image persists). Use absolute runtime values:

```dotenv
MODEL_DIR=/app/deploy/render/model
SOURCE_ARTIFACT_DIR=/app/deploy/render/assets
```

For a **native Python** service, retain the repository root as working directory,
the build/start settings in `render.yaml`, and the relative model/source paths
already listed there. Do not use Docker-only `/app` paths on the native runtime.

Once deployed, require `/health/ready` to return 200 with `artifact_ready=true`.
Then run `scripts/verify_render.py --base-url` against the service. A 200 from
`/models/current` is insufficient unless `scoring_available=true` as well.

## Free plan limits

For public-read/CORS settings, live configuration blockers, wall-clock freshness,
actual model inputs, and optional live/testnet operation, see
[remaining audit issue repairs](remaining-api-issues.md).

Render Free services sleep after 15 minutes without requests and have 512 MB
RAM; a cold request can take longer. No persistent disk is included. Included
compute, bandwidth, and build limits still apply. Any account-specific card
verification must be completed by the account owner; choosing Neon cannot
waive Render's verification policy.

Official references: [Free instances](https://render.com/docs/free),
[FastAPI deployment](https://render.com/docs/deploy-fastapi),
[Blueprint specification](https://render.com/docs/blueprint-spec).
