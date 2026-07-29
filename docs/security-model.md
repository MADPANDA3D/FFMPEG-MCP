# Security model

## Trust boundary

FFMPEG MCP accepts authenticated MCP-over-HTTP requests, reads or ingests media, places bounded jobs in Redis, executes FFmpeg in a worker, and stores resulting assets. The HTTP service, worker, Redis, storage backend, reverse proxy or Portal broker, and operator-controlled integrations are trusted deployment components. MCP clients, request bodies, remote URLs, uploaded media, filenames, and media metadata are untrusted.

The project does not claim to sandbox hostile codecs as strongly as a dedicated virtual machine boundary. Operators handling adversarial media should add host-level isolation, timely OS and FFmpeg patching, strict quotas, and a disposable worker tier.

## Authentication modes

| Mode | Credential | Intended boundary |
|---|---|---|
| `standalone` | `Authorization: Bearer ...` | Direct client to a loopback or TLS-protected endpoint |
| `portal` | Grant plus fixed `X-MADPANDA-PORTAL-SUBJECT` | Trusted private broker and stable tenant identity |

Both are service authentication. Optional Drive, S3, and standalone Discord credentials are server-side deployment secrets. In Portal mode, Discord export is the exception: the trusted broker injects the authenticated caller's BYOK token into a dedicated protected header for that one request. FFMPEG MCP strips the header before FastMCP dispatch, keeps it only in request-local memory, and never falls back to a shared Discord token.

Portal mode assumes the broker authenticates and authorizes its users, removes client-supplied protected headers, injects the trusted deployment grant and stable authenticated subject, and connects over a private network. For Discord export only, it may also inject the caller's encrypted-at-rest BYOK credential into the configured Discord token header. The subject is HMAC-derived with `MCP_PRINCIPAL_HASH_SECRET` and scopes tenant assets, jobs, caches, and brand kits. Changing that secret or a subject mapping changes the namespace; migrate or intentionally discard old state before rotating it. The supplied Portal Compose file publishes no host port.

## Primary abuse cases and controls

### Unauthenticated tool use

Authentication runs before MCP payload parsing. Missing and invalid credentials return an authorization failure without reading the body. Authenticated bodies have size and elapsed-time ceilings, and output objects are validated against the same closed schemas advertised over MCP. Startup fails when the selected mode's required secrets are absent or invalid.

### Browser and host-header abuse

Allowed hosts are explicit. Browser origins are denied unless listed by the operator. The default origin list is empty because this service is designed for MCP clients and brokers, not arbitrary browser JavaScript.

### SSRF through remote ingest

Remote ingest permits HTTPS only, requires an exact approved hostname, rejects URL credentials, resolves the target, rejects non-public addresses, revalidates every redirect, caps redirect count and elapsed time, streams with a byte ceiling, and checks the downloaded file signature. Broad-domain and HTTP bypasses fail startup validation. Keep `ALLOWED_DOMAINS` as narrow as the workflow permits. Resolution and connection are separate operations, so DNS rebinding remains a residual risk; enforce destination policy at the deployment egress layer for hostile inputs.

### Command injection

Tools map validated fields to curated FFmpeg operations and presets. There is no public arbitrary-command or arbitrary-argument tool. Contributions must retain argument-vector execution and avoid invoking a shell for media commands.

### Resource exhaustion

Inputs, outputs, durations, every probed stream, dimensions, pixels, frame rates, audio layouts, decoded-work estimates, batch sizes, expanded operation counts, graph sizes, render iterations, captions, request reads, S3 downloads, subprocess logs, and media runtimes have configurable bounds. The same central probe policy runs after ingest, before every worker media input is used, and before every generated output is persisted. Planned presets, thumbnails, logos, slideshows, templates, campaigns, renders, and workflows are checked before enqueue; non-finite numeric values and oversized list or cross-product expansions fail closed.

Before any remote-ingest HEAD or GET, an atomic Redis contract reserves one full `MAX_INGEST_BYTES` charge against dedicated owner and global staging count/byte limits. Records carry owner, unique token, original byte charge, and expiry; bounded admission scans reap expired crash leases before counting live capacity. Heartbeats can extend only a still-live matching token, release is token-fenced and idempotent, and Redis failure rejects new ingest. These keys and counters are independent from retained asset quota. The configured base lease must exceed the hard ingest + ffprobe + caller storage deadline. Cancellation during an ambiguous Redis acquisition waits for settlement and token-fenced release before it returns. A cancelled ffprobe or timed-out/cancelled ASGI storage call may leave its thread running, so a settled signal transfers the lease to a strongly referenced background finalizer until the reader or writer and any late asset cleanup release storage capacity. Process loss falls back to lease expiry.

Atomic Redis admission applies per-owner active, global active, and per-owner enqueue-per-minute limits across all queues before every job record, including cache hits. Cache-hit records immediately release active capacity while retaining their rate event. Queued work initially receives a queue-wait-plus-execution lease; a worker atomically shortens it at start to the RQ timeout plus safety margin, releases in `finally`, and has idempotent failure/stop callbacks. Failed saves and confirmed-absent enqueues roll back; an ambiguous enqueue acknowledgement retains its record and lease so already-claimed work cannot run untracked.

Asset persistence separately reserves owner/global count and bytes atomically before the backend write. Reservations are token-bound and heartbeated; only committed, active, unexpired records are visible. Reserved, active, deleting, and delete-pending assets remain charged until backend removal and token-fenced finalization release quota exactly once. Backend deletion failures enter capped exponential retry instead of dropping the control record. Each worker job also has bounded output count, output bytes, and S3 input-materialization bytes; committed partial outputs are submitted to the same deletion lifecycle when the job fails.

Async storage work in each HTTP process uses a dedicated bounded executor and semaphore with admission and caller operation deadlines. The limit is per process, so service replicas multiply aggregate concurrency, and synchronous worker paths are outside this semaphore. Timeout or cancellation does not terminate the underlying thread; the permit remains held until it finishes and any late persisted asset or temporary download is cleaned up. S3 connect/read timeouts bound SDK network inactivity but are not hard total-operation cancellation.

The asset and staging roots must be absolute, disjoint, and non-nested. Remote ingest uses the durable staging admission above. FFmpeg job outputs are separately bounded by global job admission plus per-output file-size/process limits; S3 input materialization is bounded by global job admission, per-job materialization bytes, per-object streaming caps, and bounded HTTP-process storage concurrency. The opt-in synthetic smoke uses tiny generated media and managed persistence. These already-bounded paths do not share the remote-ingest lease namespace. Periodic orphan cleanup opens the staging directory without following a symlink, scans only its top level, and removes only stale regular files after an age greater than every configured ingest/job timeout. It neither traverses directories nor follows symlinks.

FFmpeg and ffprobe run with a fixed minimal environment rather than inheriting the worker environment. The fixed `/usr/bin/prlimit` launcher from `util-linux` installs address-space, CPU-time, file-descriptor, core-dump, and FFmpeg output-file ceilings before it execs either media binary; missing or non-executable `prlimit` fails closed. FFmpeg thread options are service-controlled and bounded. Compose adds process, CPU, and memory limits. These are operational ceilings, not capacity guarantees; tune concurrency and limits from measured workloads.

### Cross-job data exposure

Media is referenced by opaque identifiers; only active, unexpired asset records are readable. Signed URL lifetime is capped by both the configured download TTL and remaining asset retention. Local signed downloads recheck lifecycle state, while an S3 presigned URL is direct backend access for its issued lifetime; a failed backend deletion can delay physical removal. Persisted records are owner-stamped or owner-keyed; cross-tenant readback fails closed. Asset lifecycle transitions and quota changes are atomic, while some job, cache, and brand-kit ownership checks remain separate from their mutations. Local storage is shared by the service and worker, and both the Docker host and backing volumes remain trusted. Local path checks and Drive upload source checks also retain a narrow check-to-use symlink race. Use separate deployments or a stronger VM/storage boundary when mutually hostile tenants must not share a trust domain.

### Queue exposure

Redis requires authentication, has no published port, and is attached only to an internal Docker network. AOF persists queues, durable asset lifecycle records, quota counters, the asset maintenance/retry index, job metadata, brand kits, caches, admission/rate state, and tenant-scoped metric keys in `redis-data`. Raw RQ arguments are bounded with explicit queue, result, and failure TTLs derived from `JOB_TTL_HOURS`, but remain sensitive until expiry and may exist in AOF/backups. Asset control records have no record TTL: uncommitted reservations require a safe abort or reap, and committed assets remain until deletion finalizes, so quota cannot be silently released while an object remains. All supplied manifests hard-cap data at 192 MiB inside a 512 MiB Redis container and require `maxmemory-policy noeviction`; the remaining memory is deliberate allocator, AOF-buffer, and background-rewrite copy-on-write headroom. An OOM therefore becomes an availability failure rather than silent state loss. Do not attach Redis to the Portal or egress networks, and do not switch to an eviction policy.

Brand-kit IDs are restricted to storage-safe values and cannot collide with the reserved tenant index. Every tenant has atomic count and serialized-record bounds; updates remain possible at the count ceiling. Kits have no record TTL because they are operator/user configuration: they persist until `brand_kit_delete` succeeds or `redis-data` is intentionally purged. Tenant metric keys use only the HMAC-derived owner namespace and have a bounded TTL. Portal responses omit service-global queue depth; standalone responses may expose it within the single-owner boundary.

### Provider side effects

Drive ingest/export and Discord export are opt-in. Drive and standalone Discord deployments require exact destination allowlists. Portal-mode Discord export uses the caller's request-scoped bot permissions instead of a global channel allowlist. Drive and Discord writes require `EXPORT TO GOOGLE DRIVE` and `EXPORT TO DISCORD`; brand-kit deletion requires `DELETE BRAND KIT`. Provider credentials exist only in the HTTP service request boundary. The worker receives Redis, storage, media, queue, and optional S3 settings only.

## Container boundary

The supplied app containers run as UID/GID `10001`, drop Linux capabilities, set `no-new-privileges`, use a read-only root filesystem, and receive writable space only through `/tmp` and the media volume. Native media commands receive a fixed `PATH`, locale, timezone, `/tmp` home and temp directory, no inherited proxy, loader, or font-configuration variables, disabled colored FFmpeg logs, and single-threaded OpenMP/OpenBLAS settings. Redis runs as its verified non-root UID/GID `999:1000`, is digest-pinned, capability-dropped, read-only outside its AOF volume, and backend-only. App and worker use a separate egress bridge for approved remote URLs, S3, and providers. Release application images are linux/amd64 and selected by digest.

FFmpeg, ffprobe, codecs, container parsers, and font/image parsers are native-code attack surfaces. Probe and decoded-work gates reduce amplification risk but cannot prove a native decoder is memory-safe. The project does not claim a VM-grade hostile-media sandbox. For adversarial media, run disposable workers inside a dedicated VM or hardened sandbox runtime such as a microVM, Kata Containers, or gVisor; avoid host filesystem mounts, apply a narrow seccomp/AppArmor policy, deny egress except required storage, keep FFmpeg and the host patched, and destroy the worker boundary after untrusted jobs.

## Operator checklist

- Generate secrets with `scripts/init_runtime_env.py`; never copy example strings into production.
- Terminate TLS at a maintained reverse proxy for any non-loopback standalone connection.
- Keep Redis and the worker off public and Portal-facing networks.
- Restrict `MCP_ALLOWED_HOSTS`, leave origins blank unless a known browser application is required, and strip spoofable forwarding headers at the edge.
- Minimize ingest domains and optional integration permissions.
- Store `.env`, Drive credentials, Redis AOF/backups, and storage keys outside source control with least-privilege filesystem access. Mount Drive JSON read-only with the documented override.
- Preserve and back up `MCP_PRINCIPAL_HASH_SECRET` under a migration-aware rotation policy.
- Monitor disk, Redis memory/OOM events, remote-ingest staging, job, and asset admission rejections, asset delete retries, queue depth, worker failure rate, storage timeouts, and output growth.
- Size HTTP storage concurrency across all service processes and replicas; do not mistake a per-process semaphore for a fleet-wide or worker-wide limit.
- Before upgrading legacy populated state, drain jobs and either selectively reset disposable asset/job state with matching media or perform an offline lifecycle/quota migration. Preserve unrelated state, never mix old and new writers, and verify consistency before admission.
- Keep probe, geometry, decoded-work, operation, rlimit, and thread ceilings at or below the documented defaults unless measured capacity and isolation justify a change.
- Route adversarial media to disposable workers behind a stronger sandbox or VM boundary; do not treat application validation as a native-code sandbox.
- Upgrade by immutable digest and retain the previous digest for rollback.
- Treat media, ffprobe output, filenames, and third-party API responses as sensitive logs; redact before sharing.
- Use private GitHub vulnerability reporting for security defects.
