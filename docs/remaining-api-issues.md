# Local repairs for audit items 2–14

These changes are committed locally only. They do not change the Render service,
Neon data, model approval, model weights, source evidence, or any blockchain.
The previously deployed readiness repair is included in this local checkout.

## Changes by audit item

2. **Unavailable model:** the Docker image includes the reviewed public bundle
   at both supported container paths and verifies checksums and installed runtime
   dependencies during the build. Model failures still fail readiness; there is
   no silent baseline fallback.
3. **Missing source files:** the image includes pinned public sources. Artifact
   verification uses that mirror; newly ingested sources can fall back to their
   permitted raw/replay path if the mirror entry is absent. A present corrupt
   mirror is never hidden by a fallback. All reads still check the database pin,
   file checksum, allowed root, symlinks, and byte budget.
4. **Empty replay discovery:** all completed replay runs are listed. Both legacy
   `replay_scenario` and worker `orchestration` metadata are recognized. Old runs
   without a usable scenario ID receive a stable `historical-run-<UUID>` display
   identity. `scenario_id_source` and `scenario_configured` distinguish a saved
   historical result from a currently configured scenario. This does not promise
   that a worker or its raw bundle is available. Completed manifests are never
   rewritten to fix discovery.
5. **Live configuration:** an opt-in baseline research profile and bounded
   northern-California region configuration are provided. The private NASA key
   is loaded from ignored `.env.firms` for local operation; environment variables
   still override dotenv files. The saved key passed NASA's read-only key-status
   check. No observations were fetched and no live regions were enabled in Neon.
6. **Historical-only approval:** model metadata exposes reviewed modes, region,
   and bbox. The scheduler refuses to queue a live run outside the selected
   model's reviewed scope. Approval is not expanded or fabricated. The opt-in
   live profile explicitly selects the existing research baseline, not the
   historical supervised forest. Software support for a baseline is not proof
   of scientific validation or physical-severity prediction.
7. **Worker/scheduler visibility:** `/status.worker` distinguishes recent job
   activity, a stalled queue, and unverified operation. It exposes a heartbeat
   timestamp and pending-job count. An empty queue is not represented as proof
   of an idle worker or scheduler running. Controlled PostgreSQL integration
   tests cover scheduling, lease fencing, worker processing, and scope rejection.
   Real continuous operation still requires starting and supervising the worker.
8. **Frontend read access:** the Render research Blueprint explicitly enables
   public sanitized reads; the global production default remains protected.
   `deploy/research-api.env.example` supplies the equivalent opt-in container
   settings. Operator POSTs and job status remain authenticated. Never expose
   the operator token in browser JavaScript.
9. **CORS:** research profiles explicitly allow localhost ports 3000 and 5173.
   Tests confirm the approved origin works and unapproved origins are rejected.
   Replace/extend `CORS_ORIGINS` with the actual deployed frontend origin before
   integration. No wildcard origins or permissive credential sharing is added.
10. **Historical/partial observations:** `is_historical` makes replay provenance
    explicit. Missing observations and partial completeness are preserved;
    generating a new processing timestamp does not make old data live. Completing
    the historical dataset requires additional source evidence, not a code flag.
11. **Freshness ambiguity:** existing cutoff-relative age/staleness fields remain
    compatible. New `wall_clock_*_age_hours` and `*_currently_stale` fields compare
    observations/weather against `evaluated_at`. An old replay can therefore be
    fresh at its cutoff but stale today without appearing to be a live feed.
12. **Uncalibrated scores:** `/models/current` exposes the verified bundle's actual
    calibration status, score units, and provisional-threshold status. Existing
    probability support remains false. Establishing calibration requires a valid
    held-out calibration/audit cohort and model review; no probability is invented.
13. **Unused enriched inputs:** metadata enumerates actual tree-split features
    and context-only inputs from the verified forest. The delivered forest uses
    nine satellite/activity features. Weather/climate skill is not claimed simply
    because those values are present in responses. A weather-sensitive model needs
    independent training/evaluation and approval; the frozen model is not altered.
14. **Disabled anchoring:** `/status.chain_readiness` exposes disabled/read-only/
    signing-configured state, network kind, blockers, and saved confirmed-record
    count. Configuration or local-chain records are not presented as independent
    notarization. No testnet setup or signing key was supplied, so external
    anchoring remains disabled. Per-alert verification remains the place to
    compare an independently read chain digest with saved evidence.

## Status contract

`data_mode` describes stored completed runs, not a streaming connection.
`live_readiness.configuration_ready` checks credential presence, enabled regions,
and scorer scope; it does not probe the provider or prove a worker is running.
`recent_live_data_available` checks observations in the published live head.
`blockers` also includes unobserved/stalled worker conditions. An old worker
heartbeat or a successful replay must not be interpreted as continuous live
monitoring. `scheduler_verified` remains false because idle scheduler liveness
cannot be proven by the current job records alone.

## Opt-in live research operation

Do not apply this profile to a shared database without reviewing the change.
The historical API continues to work independently. No live activation command
was run as part of these local code repairs.

1. Provide the private database/operator settings and the saved FIRMS key to both
   API and worker. Use `deploy/live-research.env.example` for container settings.
   For native Windows, override the container raw/model paths with local writable
   paths; the source mirror can remain `deploy/render/assets`.
2. Explicitly choose `LIVE_SCORER=baseline-v1` for the research live profile on
   both processes, retaining `ACTIVE_SCORER=registry` for historical selection,
   or use a separately approved live supervised scorer. Unset `LIVE_SCORER`
   inherits the active scorer and never silently substitutes a baseline. Inspect
   `/api/v1/status.live_readiness.scorer` for live capabilities. Do not
   broaden the historical forest's scope to bypass approval.
3. Review, then explicitly apply the opt-in region configuration with
   `python -m firewatch.cli seed-regions --config configs/regions-live-research.yaml`.
   This command writes region configuration; it was not run against Neon here.
4. Start the API and a dedicated `python -m firewatch.worker` process. The worker
   owns scheduling; never start its scheduler in an ASGI lifespan. Use a supervisor
   and persistent raw-data storage accessible to both processes at the configured
   paths. A worker on a different machine cannot provide evidence files through
   its local filesystem to the API; configure shared storage before publishing
   live results. Render Free alone does not supply a persistent
   raw volume or a separate free background-worker service. No paid resources are
   created by this change.
5. Confirm completed `mode=live` runs, recent satellite acquisition times, source
   coverage, and worker activity. Provider authentication alone does not establish
   successful live ingestion or scientific validity.

## Optional testnet anchoring

Retain `CHAIN_ENABLED=false` until an actual RPC endpoint, chain ID, deployed
registry address, confirmations policy, and private signer are configured through
secrets. Existing chain-worker/Anvil tests exercise the integration locally;
public testnet operation needs the real infrastructure. Do not commit keys or
claim that local Anvil anchors are independent public notarization.

## Verification and boundaries

Checks use the disposable **local** `firewatch_test` database and block provider
network access. A separate read-only check confirmed four replay listings,
readiness 200, nine actual forest inputs, historical scope, and current staleness
against the existing Neon records. NASA key verification used only the key-status
endpoint. No external database rows, approval records, live jobs, source evidence,
model weights, or chain transactions were changed.

The public contract and field dictionary are regenerated. Frozen historical
handoff artifacts and the earlier deployed audit remain timestamped records of
their original state. Credentials remain ignored. This commit is local only.

## Live inference follow-up (2026-10-25)

`LIVE_SCORER` selects live admission and scheduling independently of the default
historical model. The worker freezes the selected version in each job, so later
environment changes do not change a retry's scorer. Both explicit model versions
and `registry` still require reviewed live scope. There is no automatic fallback.
Status reports live scorer metadata under `live_readiness.scorer`; the existing
`scorer` field and `/models/current` retain default model semantics.

The scheduler skips unavailable approvals without preventing consumption of
existing frozen jobs, and honors smaller configured refresh windows. Provider
fetches still include the context days required for features-v1 activity windows.
Regression coverage includes API admission, registry isolation, live publication,
provider retries, weather degradation, artifact verification and wire contracts.
No live model approval, hosted configuration, region activation or Neon data was
changed by this follow-up. The forest still requires independent live evaluation.
