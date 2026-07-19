# Operator runbook

## Preflight

1. Choose `standalone` or `portal`; do not invent a third authentication path.
2. Generate `.env` with `python3 scripts/init_runtime_env.py --mode standalone`, or in Portal mode add `--public-base-url https://your-routable-origin.example`. The Portal origin must route `/download/*` to this service; a loopback or placeholder origin is not deployable.
3. Put `MCP_PRINCIPAL_HASH_SECRET` under durable secret management; changing it changes tenant namespaces and requires migration or intentional reset.
4. Set the public base URL, allowed hosts, exact ingest domains, and resource limits for the deployment. For non-container installs, install `util-linux` and verify that `/usr/bin/prlimit` is executable before starting a worker.
5. In Portal mode, set the existing private broker network and configure the broker to inject both the grant and stable `X-MADPANDA-PORTAL-SUBJECT` after stripping caller-supplied protected headers.
6. If using S3, Drive, or Discord, opt in explicitly and grant only the exact file, folder, or channel IDs required. Mount Drive credentials read-only with `examples/docker-compose.drive.override.yml`.
7. Confirm disk capacity for maximum concurrent input, staging, output, and Redis AOF growth. Keep the asset and staging roots absolute, disjoint, and non-nested; set the staging reap age above every ingest and job timeout.
8. Review stream, geometry, frame-rate, audio, decoded-work, expanded-operation, render-iteration, and caption-timing ceilings against measured capacity. Do not increase one limit without checking its cross-product with duration, presets, assets, templates, and workflows.
9. Keep FFmpeg/ffprobe address-space, CPU-time, file-descriptor, and thread limits at or below the documented defaults unless a measured workload requires more and the container/host limits leave headroom.
10. Set owner/global job admission, asset count-and-byte quotas, per-job storage budgets, enqueue RPM, and brand-kit bounds for measured capacity. Keep the admission execution buffer above the longest RQ timeout (processing timeout plus 60 seconds); retain a further safety margin.
11. Calculate HTTP storage concurrency across every service process or replica. `STORAGE_ASGI_MAX_CONCURRENCY` is per process and does not limit synchronous worker storage calls.
12. Keep Redis data at or below 192 MiB inside the supplied 512 MiB container and retain `maxmemory-policy noeviction`; the headroom is required for allocator overhead, AOF buffers, and background-rewrite copy-on-write.

## Start and verify

Standalone source deployment publishes a loopback-only host port:

```bash
docker compose up --detach --build
docker compose ps
curl --fail http://127.0.0.1:8087/health
```

Portal deployment publishes no host port. Verify it through the supplied in-stack network path:

```bash
docker compose --file docker-compose.portal.yml up --detach --build
docker compose --file docker-compose.portal.yml ps
docker compose --file docker-compose.portal.yml exec --no-TTY worker \
  python -c "import urllib.request; print(urllib.request.urlopen('http://ffmpeg-mcp:8087/health', timeout=3).read().decode())"
```

The Portal broker reaches the same `http://ffmpeg-mcp:8087/health` route from the configured private Portal network. For immutable deployments, use `docker-compose.release.yml` or `docker-compose.portal.release.yml` and omit `--build`. A healthy response must report `status: healthy`, tool count `55`, catalog `2026-07-18.4`, and the intended build provenance. Then initialize an authenticated MCP session and call `check_configuration`.

Do not treat a listening socket alone as readiness.

## Routine checks

- `docker compose ps`: server, worker, and Redis state.
- `docker compose logs --since 15m ffmpeg-mcp worker`: recent application failures and `job_enqueue_outcome_unknown` events, with secrets and media identifiers redacted before sharing. An unknown enqueue outcome intentionally retains its job record and admission lease until RQ state or expiry resolves it.
- `cleanup_deferred` / `cleanup_failed` log events and client-visible asset-quota or storage-busy/timeout errors: correlate them with backend health and retained lifecycle state. A failed deletion intentionally keeps its quota charge until backend removal and atomic finalization succeed.
- `metrics_snapshot`: tenant counters. Portal mode intentionally omits service-global queue depth.
- `job_status`, `job_progress`, and `job_logs`: one job's lifecycle.
- Host monitoring: CPU saturation, memory pressure, free disk, volume growth, container restarts, and queue latency.
- Safety-policy rejections: repeated probe, decoded-work, operation-budget, rlimit, or mandatory output-verification failures may indicate hostile media or limits that do not match the approved workload.

## Common failure sequence

1. Read `/health` and its configuration report.
2. Confirm the selected authentication mode, principal-hash secret, and required service secret are present. In Portal mode, confirm the trusted subject header is stable.
3. Confirm authenticated Redis is healthy, persisted to `redis-data`, reachable only on the backend network, below `maxmemory`, and still configured with `noeviction`. An OOM is an availability incident; do not change the eviction policy.
4. Confirm at least one worker is running and listening to the configured queue names.
5. Confirm `/data` is writable by UID/GID `10001` and has free space.
6. Run `ffmpeg -version`, `ffmpeg -L`, `ffprobe -version`, and `/usr/bin/prlimit --version` inside the image when capability, licensing, or resource-wrapper drift is suspected.
7. For ingest failures, inspect the exact allowed domain, redirect destination, resolved address class, content type, size, and detected file signature.
8. For optional exports, verify the enable flag, exact allowlist, read-only credential mount, least-privilege access, and native confirmation phrase.
9. For asset quota failures, account for reserved, active, deleting, and delete-pending records. Do not edit quota counters manually; resolve backend deletion failures and allow lifecycle finalization to release capacity.
10. For HTTP storage timeouts, account for the number of service processes, admission wait, caller operation deadline, S3 SDK timeouts, and backend latency. A caller timeout does not kill the underlying thread, and its concurrency permit remains held until that call and any late-result cleanup finish.

## Media-safety tuning

- Keep `MAX_FRAME_WIDTH`, `MAX_FRAME_HEIGHT`, and `MAX_FRAME_PIXELS` consistent; the pixel ceiling applies to every video stream and derived output.
- Treat `MAX_VIDEO_FPS` as a conservative ceiling. Probe validation uses the strongest available average, declared, or frame-count-derived rate; unknown rates consume the configured ceiling for work estimation.
- Size decoded-work ceilings from the largest approved geometry, duration, frame rate, channels, and sample rate. They are amplification budgets, not byte-size substitutes.
- `MAX_BATCH_OPERATIONS` caps expanded work across asset/preset cross-products and nested template/workflow/render plans. Keep list-specific limits lower when possible.
- Keep `FFMPEG_RLIMIT_AS_BYTES` below the worker container memory ceiling, allow headroom for the Python worker, and keep `FFPROBE_RLIMIT_AS_BYTES` lower than FFmpeg's limit.
- Keep FFmpeg CPU-time limits compatible with wall-clock timeouts. CPU time and elapsed time measure different things, so neither replaces the other.
- A mandatory output-probe failure prevents persistence. Diagnose the generated media or the probe limit; do not bypass the gate.
- For adversarial inputs, use a disposable VM or hardened sandbox worker tier with no host mounts and deny-by-default egress. The supplied container boundary is defense in depth, not a hostile-codec sandbox.

## Asset lifecycle operations

- Storage admission reserves owner/global count and bytes before a backend write. Only a committed, active, unexpired asset is visible.
- Remote ingest first reserves a full `MAX_INGEST_BYTES` against the dedicated owner/global staging limits. A rejection means active download/probe/persist leases already consume that capacity; this includes cancellation reconciliation and detached finalizers retaining admission until probe, storage, and late cleanup settle. Inspect Redis health and wait for a normal release or expired-lease reap rather than deleting lease records manually.
- Expiry or explicit removal first claims a token-fenced delete lease, then removes the backend object, then atomically releases quota. Failed backend deletes enter capped exponential retry and continue to consume quota.
- Per-job output count/bytes and S3 materialization bytes are runtime budgets. On job failure, already committed partial outputs are submitted to the same deletion lifecycle; verify retry state before assuming capacity is recovered.
- Do not manually delete Redis asset records, quota hashes, or maintenance entries independently of media objects. That can orphan objects or corrupt accounting.
- Stale staging cleanup is descriptor-anchored, non-recursive, and limited to top-level regular files older than `STORAGE_STAGING_MAX_AGE_SECONDS`; symlinks and directories are intentionally left for operator review.
- Keep `INGEST_STAGING_LEASE_SECONDS` above the validated ingest + probe + storage deadline and at least three heartbeat intervals. Owner limits must not exceed global limits, and both byte limits must admit one full `MAX_INGEST_BYTES` reservation.
- The S3 client is cached per process. Recreate service and worker processes after changing S3 credentials, endpoint, region, or client timeout settings.

## Legacy asset-state upgrade boundary

Older runtime asset records do not contain the lifecycle state, reservation token, durable quota accounting, or maintenance entries required by this release. They are not automatically migrated or visible under the new lifecycle.

Before deploying this release over populated legacy state, quiesce new submissions and let active jobs finish. Choose one application-consistent path before traffic resumes:

1. For disposable runtime state, intentionally and selectively reset legacy asset/job records and their matching media objects. Existing asset identifiers and downloads will stop working. Preserve brand kits and unrelated Redis state; deleting the entire `redis-data` volume is not a safe default.
2. If retained assets or jobs must survive, perform an offline operator-authored migration that creates valid lifecycle records, quota counters, and maintenance entries consistent with every retained backing object.

Do not mix old and new writers, manually fabricate records while live, or admit traffic until the chosen reset or migration is complete and verified.

## Upgrade

1. Read the changelog and security notes.
2. Record the current application image digest, baked release commit, baked source fingerprint, and Compose configuration.
3. Pull and scan the candidate digest in a non-production environment.
4. Run the provider-free image smoke and one synthetic media workflow.
5. Quiesce new submissions and allow active jobs to finish.
6. If upgrading from a runtime without the durable asset lifecycle, complete and verify the reset or offline migration described above before admitting traffic.
7. Update only `MCP_RELEASE_DIGEST`; release Compose must not override baked commit or source fingerprint.
8. Recreate the service and worker; verify health, catalog count, provenance, queue access, configured media/process/storage ceilings, and one synthetic result whose persisted output passes readback probing and receives a signed URL bounded by its remaining retention.
9. Retain the previous digest until the observation window closes.

## Rollback

Restore the previous digest and recreate the service and worker only when its Redis asset-record format is compatible with current state. A runtime that predates the durable lifecycle must not write against new lifecycle records. If state rollback is required, restore Redis and media from one application-consistent recovery point; never roll back `redis-data` alone or from an unrelated time because retained jobs and records may refer to different media.

## Backup boundary

`ffmpeg-data` holds local media. `redis-data` AOF holds queues, durable asset lifecycle records, quota counters, maintenance/retry state, job metadata, tenant brand kits, caches, admission/rate state, expiring tenant metrics, and raw RQ arguments. Raw RQ arguments have bounded TTLs; asset control records and their quota charges persist until a safe reservation abort or deletion finalization, and brand kits persist until `brand_kit_delete` succeeds or the volume is purged. Back up either volume only when policy requires it, encrypt backups, apply the same deletion schedule as live data, and test Redis plus media restoration as one application-consistent set. Back up authentication, principal-hash, and signing secrets through a secret manager. Do not copy `.env`, credentials, AOF files, production logs, or customer media into the repository or issue tracker. `docker compose down --volumes` permanently deletes both named volumes.
