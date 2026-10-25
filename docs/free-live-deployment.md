# Free scheduled live research deployment

Render serves the API, Neon stores records, Supabase stores private source bytes,
and GitHub Actions runs a bounded hourly refresh. This is best-effort scheduled
research scoring, not continuous monitoring or an approved live forest model.
The public repository's standard Linux runners avoid private-repository minute
charges. Storage, database and bandwidth allowances still apply; never enable
paid upgrades or automatic storage deletion to hide quota problems.

## 1. Deploy the reviewed code

The API and scheduled worker must both contain the shared-storage changes.
Merge the reviewed commit into the repository's default branch, then deploy that
same code on Render. GitHub schedules only run workflows present on the default
branch. The local changes are not pushed automatically by this setup.

## 2. Private Supabase settings

Create a private bucket named `firewatch-evidence`. Save local credentials in
ignored `.env.supabase`:

```dotenv
SUPABASE_URL=https://YOUR-PROJECT-REF.supabase.co
SUPABASE_SECRET_KEY=REPLACE_WITH_SERVER_SECRET_KEY
SUPABASE_BUCKET=firewatch-evidence
SUPABASE_STORAGE_ENABLED=false
```

The modern server secret (`sb_secret_...`) is sent only as an `apikey` header;
the anon/publishable key is insufficient. Do not create public bucket policies.
Local storage remains disabled unless explicitly enabled. To check credentials
without changing application data:

```powershell
.\.venv\Scripts\python.exe scripts/verify_supabase.py
```

`--round-trip` additionally uploads, verifies and deletes one unique synthetic
object. It never uploads historical data or edits Neon. Do not expose this key in
a frontend, Git commit, screenshot, log or chat.

## 3. Configure the existing Render Docker API

Keep its existing `DATABASE_URL`, `ADMIN_API_TOKEN` and other deployment values.
Add the following in Render's Environment settings and redeploy:

```dotenv
SUPABASE_STORAGE_ENABLED=true
SUPABASE_URL=https://YOUR-PROJECT-REF.supabase.co
SUPABASE_SECRET_KEY=REPLACE_WITH_SERVER_SECRET_KEY
SUPABASE_BUCKET=firewatch-evidence
ACTIVE_SCORER=registry
LIVE_SCORER=baseline-v1
MODEL_DIR=/app/deploy/render/model
SOURCE_ARTIFACT_DIR=/app/deploy/render/assets
WORKER_HEALTH_MAX_LAG_SECONDS=7200
```

The API command remains the Dockerfile default, Uvicorn on port 8000. Never change
the web-service command to the worker. The worker below runs separately on GitHub.
The packaged historical mirror remains available; only absent sources use private
storage. A present corrupt local or remote object is reported, never overwritten.

## 4. Set GitHub Actions secrets

In the repository that will run inference, open Settings > Secrets and variables
> Actions > Secrets. Add these **repository secrets**, copying the same Neon URL
and operator token used by the API:

* `DATABASE_URL` (must use `postgresql+psycopg://`)
* `ADMIN_API_TOKEN`
* `FIRMS_MAP_KEY`
* `SUPABASE_URL`
* `SUPABASE_SECRET_KEY`

The bucket name is already configured in `.github/workflows/live-inference.yml`.
No Supabase database password, anon key, GitHub PAT or Render API token is needed.
Do not paste secrets into the YAML. Only trusted maintainers should edit this
workflow or the Python it runs, because they execute with these credentials.

## 5. Check regions, then opt in

Your existing Northern California region must already be enabled in Neon. The
workflow does not seed regions or run migrations. To enable this region yourself
if needed (writes to the configured database):

```powershell
.\.venv\Scripts\python.exe -m firewatch.cli seed-regions --config configs/regions-live-research.yaml
```

Stop the PC worker with Ctrl+C when transferring scheduling to GitHub, so its
15-minute scheduler does not keep creating extra jobs alongside the hourly one.
In GitHub Settings > Secrets and variables > Actions > Variables, add:

```text
LIVE_INFERENCE_ENABLED=true
```

Open Actions > Scheduled live research inference > Run workflow on the reviewed
default branch. Confirm it succeeds before relying on scheduled runs. The workflow
runs at minute 17 each hour UTC, processes at most three due live jobs, and has a
20-minute timeout. It never consumes historical replay jobs. Failed/incomplete
processed jobs return a failing workflow status; retries use the same frozen pins.

## 6. Acceptance checks

* `/health/ready` returns 200 for the historical scorer.
* `/api/v1/status` shows `live_readiness.scorer.scorer_version=baseline-v1`.
* `/api/v1/runs` contains a completed `mode=live` run after the workflow.
* Satellite acquisition timestamps are recent; retrieval time alone is insufficient.
* If critical alerts exist, their artifact verification succeeds on Render after
  the GitHub runner exits. No detections or alerts can be a valid source result.

Source bytes are uploaded and read back by checksum before publishing the live
head, including non-critical runs. Foreign workstation paths are not followed or
rewritten. Fresh runners can recover cached weather and checkpointed source pins
from private storage. Existing completed PC runs are not automatically backfilled;
their missing raw evidence may remain unavailable until explicitly uploaded.

## Limits and stopping

Render Free sleeps without requests; GitHub schedules may be delayed or dropped,
and public-repository schedules disable after 60 days without repository activity.
These services do not promise 24/7 operation. Watch Supabase storage/egress and
Neon usage; evidence grows with every refresh and is not deleted automatically.
Disable `LIVE_INFERENCE_ENABLED` or the workflow to stop new scheduled refreshes.
This does not interrupt an already running batch. Leave bucket access private.

References: [GitHub Actions billing](https://docs.github.com/en/billing/concepts/product-billing/github-actions),
[scheduled workflow behavior](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule),
[Supabase storage pricing](https://supabase.com/pricing),
[Render Free limits](https://render.com/docs/free).
