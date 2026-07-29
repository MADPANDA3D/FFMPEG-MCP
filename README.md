<p align="center">
  <img src="assets/brand/header.jpg" alt="MADPANDA3D FFMPEG MCP" width="100%" />
</p>

<h1 align="center">FFMPEG MCP</h1>

<p align="center">
  A bounded, asynchronous media engine for MCP clients — built on FFmpeg, Redis, and Python.
</p>

<p align="center">
  <a href="https://github.com/MADPANDA3D/FFMPEG-MCP/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/MADPANDA3D/FFMPEG-MCP/ci.yml?branch=main&label=verify" alt="verification status" /></a>
  <a href="https://github.com/MADPANDA3D/FFMPEG-MCP/releases"><img src="https://img.shields.io/github/v/release/MADPANDA3D/FFMPEG-MCP?display_name=tag&color=ff304f" alt="latest release" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-5f5fff" alt="MIT license" /></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/python-3.12%20%7C%203.13-3776ab" alt="Python 3.12 and 3.13" /></a>
  <a href="https://modelcontextprotocol.io/"><img src="https://img.shields.io/badge/MCP-streamable%20HTTP-111827" alt="MCP Streamable HTTP" /></a>
</p>

```text
╭────────────────────────── MEDIA CONTROL PLANE ──────────────────────────╮
│  INGEST  →  PROBE  →  QUEUE  →  RENDER  →  VERIFY  →  EXPORT          │
│                                                                        │
│  55 named tools · deterministic catalog · standalone or Portal auth    │
╰────────────────────────────────────────────────────────────────────────╯
```

FFMPEG MCP turns repeatable video, audio, image, caption, template, and export operations into named MCP tools. Heavy media work runs through Redis Queue workers, while the HTTP service stays focused on authenticated discovery, validation, job submission, and status.

This is a self-hosted media service, not a shell wrapper. Tools accept bounded schemas and curated presets; clients do not receive arbitrary command execution.

## What ships

- **55 deterministic tools** covering ingest, transcode, captions, overlays, audio, templates, campaigns, exports, jobs, and discovery.
- **Two deployment modes** with the same tool contract: standalone bearer authentication or private Portal grant authentication.
- **Asynchronous processing** through authenticated, AOF-backed Redis and one or more RQ workers.
- **Local or S3-compatible storage**, signed download links, and optional Google Drive and Discord integrations.
- **Durable asset lifecycle controls** with atomic owner/global count-and-byte quotas, token-fenced deletion retries, and bounded per-job storage budgets.
- **Bounded async storage admission** through a dedicated per-process executor and semaphore, with caller deadlines and late-result cleanup.
- **Central media safety gates** that validate every stream, decoded-work estimate, planned output, worker input, and persisted output against one finite policy.
- **Contained native processes** with a fixed minimal environment, bounded FFmpeg threads, and Linux address-space, CPU, file-descriptor, core-dump, and output-file limits.
- **Hardened containers** running as UID/GID `10001`, with a read-only root filesystem, dropped capabilities, resource limits, separated server/worker secrets, and isolated backend and egress networks.
- **Verified Linux releases** with a locked Python environment, immutable linux/amd64 GHCR digest deployment, source fingerprints, checksums, and attestations.

## Architecture

```mermaid
flowchart LR
  C["MCP client"] -->|"Bearer token"| S["FFMPEG MCP"]
  P["Private MCP broker"] -->|"Portal grant"| S
  S -->|"enqueue and inspect"| R[("Redis")]
  W["RQ worker"] -->|"claim jobs"| R
  W --> F["FFmpeg / ffprobe"]
  W --> D[("Shared media storage")]
  S --> D
  D -. optional .-> O["S3 / Drive / Discord"]
```

Redis is internal-only in every supplied Compose topology. The Portal topology exposes the MCP service only to a pre-existing private broker network. The standalone topology binds to loopback by default.

## Five-minute standalone start

Requirements: a `linux/amd64` host (or explicitly configured amd64 emulation), Docker Engine with Compose v2, Git, and enough local disk for your media limits.

```bash
git clone https://github.com/MADPANDA3D/FFMPEG-MCP.git
cd FFMPEG-MCP
python3 scripts/init_runtime_env.py --mode standalone
docker compose up --detach --build --wait
curl --fail http://127.0.0.1:8087/health
```

The initializer creates an ignored mode-`0600` `.env` with independent access, principal-hash, Redis, and download-signing secrets. It never prints a secret and never overwrites an existing file. Preserve `MCP_PRINCIPAL_HASH_SECRET`: changing it changes every tenant namespace and makes existing tenant-owned records inaccessible until deliberately migrated.

Point a Streamable HTTP MCP client at `http://127.0.0.1:8087/mcp` and send:

```http
Authorization: Bearer YOUR_MCP_ACCESS_TOKEN
```

A representative client configuration is:

```json
{
  "mcpServers": {
    "ffmpeg": {
      "type": "streamable-http",
      "url": "http://127.0.0.1:8087/mcp",
      "headers": {
        "Authorization": "Bearer YOUR_MCP_ACCESS_TOKEN"
      }
    }
  }
}
```

Use your client's secret store or environment interpolation when available; do not commit a real token to client configuration.

Stop the stack with `docker compose down`. The named `ffmpeg-data` and `redis-data` volumes are retained unless you explicitly remove them.

## Portal mode

Portal mode is for a private broker that authenticates users upstream and injects a deployment grant plus a stable authenticated subject header. It is not an unauthenticated public mode.

```bash
python3 scripts/init_runtime_env.py \
  --mode portal \
  --public-base-url https://ffmpeg-mcp.example.com
```

Replace the example origin with the real externally reachable origin that routes `/download/*` to this service. The initializer refuses Portal mode without it; loopback and placeholder origins are not valid production configuration.

Set `MCP_PORTAL_NETWORK` in `.env` to the name of the broker's existing private Docker network, then start:

```bash
docker compose --file docker-compose.portal.yml up --detach --build --wait
```

The broker connects to `http://ffmpeg-mcp:8087/mcp` on that network and sends both protected headers:

```http
X-MADPANDA-PORTAL-GRANT: YOUR_MCP_PORTAL_GRANT_TOKEN
X-MADPANDA-PORTAL-SUBJECT: YOUR_STABLE_AUTHENTICATED_TENANT_ID
```

`X-MADPANDA-PORTAL-SUBJECT` is fixed, not operator-configurable. Its stable broker-authenticated value becomes the tenant identity used for assets, jobs, caches, and brand kits. The Portal manifest publishes no host port. Keep the broker network private; strip client-supplied grant, subject, authorization, cookie, and forwarding headers, then inject the trusted grant and subject only after upstream authorization. Changing a subject mapping or `MCP_PRINCIPAL_HASH_SECRET` is a data-identity migration, not a routine credential rotation.

## Immutable release deployment

Source builds are for development and review. Production operators should select a release, obtain its exact GHCR digest, and use the matching digest-only manifest:

```bash
docker compose --file docker-compose.release.yml up --detach --wait
# or
docker compose --file docker-compose.portal.release.yml up --detach --wait
```

The release manifests require only `MCP_RELEASE_DIGEST` for image selection and never accept a mutable tag. Build commit and source fingerprint remain baked into the image and cannot be replaced by deployment environment values; `/health` reads them back for comparison with the release. Published images target `linux/amd64`. See [docs/provenance.md](docs/provenance.md) for verification.

## Tool catalog

| Surface | Count | Tools |
|---|---:|---|
| Ingest and storage | 6 | URL/Drive ingest, probing, signed downloads, Drive/Discord export |
| Core video | 7 | transcode, thumbnail, trim, text, logo, captions, concat |
| Analysis and QA | 4 | analyze, compare, list and describe rubrics |
| Image to video | 3 | still video, slideshow, Ken Burns slideshow |
| Audio | 7 | extract, normalize, mix, duck, background mix, fade, trim silence |
| Templates and brand kits | 8 | template discovery/apply and brand-kit lifecycle/apply |
| Batch and workflow | 8 | format batches, campaigns, ads, testimonials, offer cards, iteration, workflow |
| Meta, presets, and jobs | 12 | configuration, discovery, coverage, usage, presets, capabilities, status, logs, metrics |
| **Total** | **55** | Deterministic catalog `2026-07-18.4` |

The full, copyable list with behavior notes is in [docs/tool-catalog.md](docs/tool-catalog.md). Agents should begin with `list_capabilities`, narrow with `find_tools`, and read `get_tool_usage` before invoking unfamiliar tools.

Most rendering tools return a job identifier. Poll `job_status` or `job_progress`, inspect `job_logs` when needed, and request a signed URL only after an output asset is ready.

## Configuration

The safe baseline lives in [.env.example](.env.example). Blank secret fields are intentional.

| Variable | Purpose | Safe default |
|---|---|---|
| `MCP_MODE` | `standalone` or `portal` authentication contract | `standalone` |
| `MCP_ACCESS_TOKEN` | Standalone bearer credential | blank; required in standalone mode |
| `MCP_PORTAL_GRANT_TOKEN` | Broker-to-server grant credential | blank; required in Portal mode |
| `X-MADPANDA-PORTAL-SUBJECT` | Fixed trusted Portal request header carrying the stable tenant ID | broker-supplied in Portal mode |
| `MCP_DISCORD_TOKEN_HEADER` | Protected broker header carrying the calling user's Discord BYOK token for one export request | `X-Discord-Bot-Token` |
| `MCP_PRINCIPAL_HASH_SECRET` | Stable HMAC key for tenant namespaces | blank; required, migration-sensitive |
| `MCP_ALLOWED_HOSTS` | Accepted HTTP Host values | local/container names only |
| `MCP_ALLOWED_ORIGINS` | Browser origins allowed to call MCP | blank |
| `MCP_REQUEST_BODY_MAX_BYTES` / `MCP_REQUEST_BODY_TIMEOUT_SECONDS` | Authenticated request size and read deadline | `131072` / `10` |
| `MCP_RATE_LIMIT_PRINCIPAL_RPM` | Per-principal request ceiling | `300` |
| `REDIS_PASSWORD` / `REDIS_URL` | Authenticated queue and persisted metadata store | blank secret / authenticated internal URL |
| `REDIS_MAXMEMORY_BYTES` | Redis data ceiling; Compose enforces `noeviction` | `201326592` (192 MiB) |
| `JOB_ADMISSION_OWNER_MAX_ACTIVE` / `JOB_ADMISSION_GLOBAL_MAX_ACTIVE` | Atomic active-job reservations across all queues | `4` / `32` |
| `JOB_ADMISSION_OWNER_RPM` | Separate per-tenant enqueue ceiling | `30` |
| `JOB_ADMISSION_EXECUTION_BUFFER_SECONDS` | Crash-reservation time after queue TTL | `3720` |
| `METRICS_TTL_SECONDS` | Tenant metric-key retention | `86400` |
| `BRAND_KIT_MAX_COUNT` / `BRAND_KIT_MAX_SERIALIZED_BYTES` | Per-tenant kit count and record size | `25` / `16384` |
| `PUBLIC_BASE_URL` | Exact origin for signed local download links; Portal mode must be externally reachable | loopback standalone example |
| `DOWNLOAD_SIGNING_SECRET` | HMAC secret for local download links | blank; minimum 32 characters |
| `DOWNLOAD_URL_TTL_SECONDS` | Maximum signed-link lifetime, also capped by remaining asset retention | `3600` |
| `ALLOWED_DOMAINS` | Exact domains allowed for remote ingest | narrow media list |
| `MAX_INGEST_BYTES` | Maximum downloaded input size | `500000000` |
| `INGEST_STAGING_OWNER_MAX_ACTIVE` / `INGEST_STAGING_GLOBAL_MAX_ACTIVE` | Atomic concurrent remote-ingest staging reservations | `2` / `8` |
| `INGEST_STAGING_OWNER_MAX_BYTES` / `INGEST_STAGING_GLOBAL_MAX_BYTES` | Conservative staging byte admission; each remote ingest reserves `MAX_INGEST_BYTES` | `1000000000` / `4000000000` |
| `INGEST_STAGING_LEASE_SECONDS` / `INGEST_STAGING_HEARTBEAT_SECONDS` | Crash-safe remote-ingest lease and refresh interval | `600` / `30` |
| `MAX_OUTPUT_BYTES` | Maximum produced asset size | `500000000` |
| `MAX_DURATION_SECONDS` | Maximum accepted media duration | `3600` |
| `MAX_FRAME_WIDTH` / `MAX_FRAME_HEIGHT` / `MAX_FRAME_PIXELS` | Per-video-stream and derived-output geometry ceilings | `8192` / `8192` / `33177600` |
| `MAX_MEDIA_STREAMS` / `MAX_VIDEO_FPS` | Stream-count and conservative frame-rate ceilings | `16` / `120` |
| `MAX_AUDIO_CHANNELS` / `MAX_AUDIO_SAMPLE_RATE` | Per-audio-stream layout and sample-rate ceilings | `8` / `192000` |
| `MAX_DECODED_VIDEO_PIXEL_FRAMES` / `MAX_DECODED_AUDIO_SAMPLE_CHANNELS` | Aggregate decoded-work ceilings per validated media object or planned output | `250000000000` / `6000000000` |
| `MAX_BATCH_OPERATIONS` / `MAX_RENDER_ITERATIONS` | Expanded pre-enqueue operation and iterative-render ceilings | `100` / `5` |
| `MAX_CAPTION_WORD_TIMINGS` | Maximum timed caption words accepted by one request | `2000` |
| `FFMPEG_RLIMIT_AS_BYTES` / `FFPROBE_RLIMIT_AS_BYTES` | Native subprocess address-space ceilings | `3221225472` / `536870912` |
| `FFMPEG_RLIMIT_CPU_SECONDS` / `FFPROBE_RLIMIT_CPU_SECONDS` | Native subprocess CPU-time ceilings | `1800` / `60` |
| `MEDIA_RLIMIT_NOFILE` / `FFMPEG_THREADS` | Native subprocess file-descriptor and FFmpeg thread ceilings | `256` / `2` |
| `STORAGE_BACKEND` | `local` or S3-compatible object storage | `local` |
| `STORAGE_LOCAL_DIR` / `STORAGE_TEMP_DIR` / `STORAGE_STAGING_MAX_AGE_SECONDS` | Absolute, disjoint asset/staging roots and stale top-level staging-file age | `/data/assets` / `/data/staging` / `7200` |
| `S3_CONNECT_TIMEOUT_SECONDS` / `S3_READ_TIMEOUT_SECONDS` | S3 SDK connection and read inactivity timeouts | `10` / `60` |
| `STORAGE_ASGI_MAX_CONCURRENCY` / `STORAGE_ASGI_ADMISSION_TIMEOUT_SECONDS` / `STORAGE_ASGI_OPERATION_TIMEOUT_SECONDS` | Per-HTTP-process storage-call concurrency, admission wait, and caller operation deadline | `4` / `5` / `120` |
| `ASSET_TTL_HOURS` / `MAX_ASSET_TTL_HOURS` | Default and maximum asset retention | `24` / `168` |
| `ASSET_QUOTA_OWNER_MAX_COUNT` / `ASSET_QUOTA_OWNER_MAX_BYTES` | Per-owner retained asset count and bytes | `100` / `5368709120` (5 GiB) |
| `ASSET_QUOTA_GLOBAL_MAX_COUNT` / `ASSET_QUOTA_GLOBAL_MAX_BYTES` | Service-wide retained asset count and bytes | `400` / `21474836480` (20 GiB) |
| `ASSET_RESERVATION_LEASE_SECONDS` / `ASSET_RESERVATION_HEARTBEAT_SECONDS` | Backend-write reservation lease and heartbeat | `300` / `30` |
| `ASSET_DELETE_LEASE_SECONDS` / `ASSET_DELETE_RETRY_BASE_SECONDS` / `ASSET_DELETE_RETRY_MAX_SECONDS` | Delete claim lease and exponential retry bounds | `180` / `60` / `3600` |
| `JOB_STORAGE_MAX_OUTPUT_COUNT` / `JOB_STORAGE_MAX_OUTPUT_BYTES` / `JOB_STORAGE_MAX_MATERIALIZE_BYTES` | Per-job output count, output bytes, and S3 input-materialization bytes | `25` / `2147483648` / `2147483648` |

Google Drive, S3, and standalone Discord credentials are deployment secrets owned by the operator. Portal-mode Discord export instead requires the calling user's request-scoped BYOK token from the trusted broker; it is never stored by FFMPEG MCP or forwarded into FastMCP. Leave optional fields blank when integrations are not used. Never put secrets in MCP arguments, issue reports, logs, or commits.

Drive ingest and export and Discord export are disabled by default. Drive and standalone Discord modes require exact file/folder/channel allowlists. Portal-mode Discord destinations are bounded by the caller's own bot permissions and still require an explicit numeric channel ID. Provider writes require native confirmation arguments: `EXPORT TO GOOGLE DRIVE` or `EXPORT TO DISCORD`; deleting a brand kit requires `DELETE BRAND KIT`.

For Drive, keep the credential JSON outside the repository and mount it read-only with the supplied override:

```bash
export GOOGLE_DRIVE_CREDENTIALS_FILE=/absolute/operator-owned/google-drive.json
docker compose --file docker-compose.yml \
  --file examples/docker-compose.drive.override.yml up --detach --build
```

The local `secrets/` directory and common credential JSON names are ignored by Git and the Docker build context. The worker never receives access, Portal, principal-hash, download-signing, Drive, or Discord secrets.

## Storage and scaling

The supplied stack shares `/data` between the HTTP service and worker. The `ffmpeg-data` volume stores media; the authenticated `redis-data` volume uses AOF for queues, job and asset metadata, tenant brand kits, caches, and rate-limit state. Both are appropriate for one trusted Docker host. Multiple workers may share Redis and the media volume on that host.

Each asset is atomically reserved in Redis before its backend write, charging both owner and global count-and-byte quotas. A heartbeat keeps an in-progress reservation alive; successful persistence commits it to `active`. Only active, unexpired assets are visible. Asset control records and their quota charges do not disappear on a Redis record TTL: they remain until an uncommitted reservation is safely aborted or deletion is finalized.

Expiry and explicit cleanup use a token-fenced delete claim. Backend deletion happens before the Redis record and quota charge are finalized. A backend failure moves the asset into a retryable pending state with capped exponential backoff, so reserved, active, deleting, and delete-pending assets continue to consume quota until deletion succeeds. Do not treat a delete request or expired retention time as immediately recovered capacity.

Worker jobs also have in-process budgets for output count, total output bytes, and S3 input materialization bytes. Output budget is charged before persistence, and a failed job attempts to remove already committed partial outputs through the same deletion lifecycle. These job budgets complement durable owner/global quotas; they do not replace them.

Signed download URLs cannot be issued for expired assets and are limited to the smaller of `DOWNLOAD_URL_TTL_SECONDS` and remaining asset retention. Local signed downloads recheck active/unexpired state. An S3 presigned URL grants direct backend access for its issued lifetime; retention bounds that lifetime at issuance, but a backend deletion failure can delay physical object removal.

Async storage calls in the HTTP service use a dedicated bounded executor and semaphore. `STORAGE_ASGI_MAX_CONCURRENCY` is per Python service process, so total potential concurrency grows with the number of service processes or replicas. The operation timeout is a caller deadline, not a hard thread cancellation: a timed-out or cancelled call retains its permit until the underlying SDK/filesystem call finishes and late-result cleanup completes. Synchronous worker storage calls are outside this ASGI semaphore and remain governed by job admission, worker concurrency, SDK timeouts, and storage quotas. The S3 client is cached per process; restart service and worker processes after changing S3 credentials or client settings.

Every remote URL/Drive ingest atomically reserves one full `MAX_INGEST_BYTES` charge against dedicated owner and global staging limits before the first HEAD or GET. The token-fenced Redis lease is heartbeated during download, probe, and persistence, fails closed when admission is unavailable, and is independent from retained managed-asset quota. Cancellation during an ambiguous Redis acquisition is reconciled before returning. If cancellation or a caller deadline leaves probe or storage work running, a strongly referenced background finalizer retains the lease until that work and any late cleanup settle. The cleanup service also removes stale top-level regular files from the staging root after `STORAGE_STAGING_MAX_AGE_SECONDS`. The age must exceed every configured ingest and job timeout. Cleanup is non-recursive and does not follow symlinks, so nested directories and linked paths require operator review rather than automatic deletion.

Tenant-owned records are keyed or stamped with a one-way HMAC of the authenticated principal. Cross-tenant lookups fail closed, but Redis remains a trusted shared database, not a hostile multi-tenant isolation boundary. Application records use a decoded Redis client; RQ uses a separate binary-safe client for serialized job payloads. Raw RQ function arguments can remain in Redis until the configured queued, result, or failure TTL expires; all three are bounded by `JOB_TTL_HOURS`. AOF files and backups may therefore contain identifiers and job inputs. Protect, retain, and delete `redis-data` under the same privacy policy as application state. `docker compose down --volumes` permanently deletes both named volumes.

Job admission uses one atomic Redis operation before every job record, including cache-hit records. It purges expired reservations and enforces per-tenant active, service-wide active, and per-tenant enqueue-rate limits across every configured queue. Cache-hit records release active capacity immediately while retaining the rate event. Queued work initially reserves the queue TTL plus `JOB_ADMISSION_EXECUTION_BUFFER_SECONDS`; at worker start the lease is shortened to the actual RQ timeout plus a 60-second safety margin. Workers release in `finally`, and terminal RQ failure/stop callbacks reap reservations idempotently. A failed enqueue rolls back only when RQ can prove the job is absent; an ambiguous lost acknowledgement retains the job record and lease rather than risking untracked execution.

Brand kits persist in `redis-data` until explicitly deleted or the volume is purged. IDs are storage-safe and reserved index names are rejected; per-tenant count, string, and serialized-record limits keep list responses bounded without pagination. Deleting a kit removes both its record and tenant index entry atomically. Removing `redis-data`, including `docker compose down --volumes`, permanently removes all brand kits.

All supplied manifests hard-cap Redis data at 192 MiB inside a 512 MiB container and use `maxmemory-policy noeviction`. The extra container headroom is intentional for allocator overhead, AOF buffers, and copy-on-write during background rewrites. When Redis cannot accept a required write, queue and brand-kit operations fail as unavailable instead of silently evicting tenant state. Treat Redis OOM/restarts as availability incidents and tune admission limits or move to a separately sized Redis deployment rather than enabling eviction.

Metric keys include only the HMAC-derived tenant owner and expire after `METRICS_TTL_SECONDS`. Portal mode returns tenant counters with an empty `queue_depth` object because RQ queue counts are service-global; standalone mode may report those global counts because it has one authenticated owner boundary.

For multi-host workers, use shared S3-compatible storage and ensure every worker has compatible FFmpeg capabilities and fonts. Redis and S3 traffic use the app/worker egress bridge, while Redis itself remains only on the internal backend. Durable media belongs in the configured storage backend; Redis persistence does not replace media backups.

Review resource and concurrency limits against the largest permitted input before accepting untrusted work. FFmpeg is CPU-, memory-, and disk-intensive even when schemas are bounded.

Media is re-probed at ingest, before worker use, and before an output is persisted. The gate inspects every stream, uses conservative frame-rate evidence, preserves durationless still images, rejects non-finite metadata, and caps aggregate decoded video and audio work. Pre-enqueue validation separately bounds list sizes, nested template/workflow expansion, campaign cross-products, render iterations, caption timings, and derived preset or thumbnail geometry. A failed mandatory output probe fails closed before storage metadata is committed.

### Legacy asset-state upgrade boundary

This lifecycle does not automatically migrate asset records created by older runtimes that lack lifecycle state, reservation tokens, durable quota counters, or the maintenance index. Do not start the new runtime over populated legacy asset state and assume old asset identifiers will remain usable, and do not run legacy and new writers against the same Redis state.

Before upgrading, stop new submissions and let active jobs finish. For disposable runtime assets, the simplest path is an intentional, selective reset of legacy asset/job records and their matching media objects; old asset identifiers and downloads will become unavailable. Preserve brand kits and any unrelated Redis state rather than treating deletion of the entire `redis-data` volume as a safe default. If retained assets or jobs must survive, perform an offline, operator-authored migration that creates valid lifecycle records, quota counters, and maintenance entries consistent with the backing objects before admitting traffic. Do not fabricate or repair these records while the service is live.

## Local Python development

The Python distribution does not bundle FFmpeg, Redis, fonts, durable storage, or the resource-limit launcher. The runtime requires Linux and the fixed `/usr/bin/prlimit` executable supplied by `util-linux`; FFmpeg and ffprobe are launched through that wrapper so limits exist before either media process starts. Python 3.12 and 3.13 are supported on Linux. Install FFmpeg, fonts, `util-linux`, and your Redis/storage dependencies separately. Published containers are linux/amd64 only and explicitly install and verify `/usr/bin/prlimit`.

```bash
uv lock --check
uv sync --frozen --group dev
uv export --frozen --no-dev --no-emit-project \
  --format requirements-txt --output-file requirements.lock
git diff --exit-code -- requirements.lock
uv run python -m compileall -q src scripts tests
uv run pytest -q -p no:cacheprovider
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run bandit -q -r src/madpanda_ffmpeg_mcp -lll
uv run pip-audit --requirement requirements.lock
uv run python scripts/check_source_safety.py
```

Render all four Compose variants in a disposable shell with non-production validation values:

```bash
export MCP_ACCESS_TOKEN=verify-only-access-token-000000000000000000000000
export MCP_PORTAL_GRANT_TOKEN=verify-only-portal-grant-00000000000000000000000
export MCP_PRINCIPAL_HASH_SECRET=verify-only-principal-secret-000000000000000000000
export DOWNLOAD_SIGNING_SECRET=verify-only-download-secret-0000000000000000000000
export REDIS_PASSWORD=verify-only-redis-password-000000000000000000000000
export MCP_PORTAL_NETWORK=verify-only-private-network
export PUBLIC_BASE_URL=https://ffmpeg-mcp.example.invalid
export MCP_RELEASE_DIGEST=0000000000000000000000000000000000000000000000000000000000000000
docker compose --file docker-compose.yml config --quiet
docker compose --file docker-compose.portal.yml config --quiet
docker compose --file docker-compose.release.yml config --quiet
docker compose --file docker-compose.portal.release.yml config --quiet
unset MCP_ACCESS_TOKEN MCP_PORTAL_GRANT_TOKEN MCP_PRINCIPAL_HASH_SECRET \
  DOWNLOAD_SIGNING_SECRET REDIS_PASSWORD MCP_PORTAL_NETWORK PUBLIC_BASE_URL \
  MCP_RELEASE_DIGEST
```

Build the wheel and source archive with:

```bash
uv build
uv run twine check dist/*
```

Release wheels expose `mad-mcp-ffmpeg` and `mad-mcp-ffmpeg-worker` console commands.

## Security posture

- Authentication is evaluated before MCP payload parsing.
- Standalone and Portal credentials are separate and cannot be enabled as an unauthenticated fallback.
- Browser-origin requests are denied unless explicitly allowed.
- URL ingest is HTTPS-only, requires exact approved domains, rejects private DNS results, revalidates redirects, streams with byte limits, and validates media by file signature. DNS rebinding remains a residual risk; enforce deployment-level egress policy for hostile inputs.
- Inputs map to curated operations; clients cannot submit arbitrary FFmpeg commands.
- Every media input and persisted output is centrally re-probed, and planned geometry plus decoded work must fit configured ceilings before enqueue.
- FFmpeg and ffprobe receive a fixed minimal environment; native subprocesses have bounded address space, CPU time, descriptors, core dumps, output size, and FFmpeg thread count.
- Tool return values are validated against the same closed schemas advertised over MCP.
- Redis is authenticated, persisted, and never published by the provided manifests.
- Containers run non-root with dropped capabilities and read-only root filesystems.
- FFmpeg/ffprobe parsers and codecs remain a native-code attack surface; for adversarial media, use disposable workers behind a VM or hardened sandbox boundary, with no host mounts and deny-by-default egress.
- CI scans source, Git history, workflows, dependencies, packages, and images before release.

Read the complete boundary and operator checklist in [docs/security-model.md](docs/security-model.md). Report vulnerabilities privately according to [SECURITY.md](SECURITY.md).

## Operations

Use `GET /health` for liveness, configuration readiness, the exact tool count, and build provenance. Use `check_configuration` for an agent-readable configuration report and `metrics_snapshot` for service counters. The public operator sequence, upgrade procedure, and failure triage are in [docs/operator-runbook.md](docs/operator-runbook.md).

## Project policy

- [Contributing](CONTRIBUTING.md)
- [Support](SUPPORT.md)
- [Security](SECURITY.md)
- [Changelog](CHANGELOG.md)
- [License](LICENSE)
- [Third-party notice](NOTICE)

FFmpeg is a trademark of Fabrice Bellard, originator of the FFmpeg project. This independent project is not affiliated with or endorsed by the FFmpeg project.
