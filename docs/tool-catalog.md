# Tool catalog

Catalog version: `2026-07-18.4`

Public tool count: **55**

Legacy tools: **0**
Hidden tools: **0**

Tool descriptors, annotations, argument schemas, and descriptor hashes are generated from the runtime registry. Use `list_capabilities`, `find_tools`, `get_tool_usage`, and `get_endpoint_coverage` for machine-readable discovery.

## Behavior contract

| Surface | Read/write behavior | Network boundary | Result |
|---|---|---|---|
| Discovery, presets, rubrics, configuration, metrics, job readback | Read-only and idempotent | Local service/Redis only | Immediate structured object |
| Ingest and probe | Writes tenant asset data/metadata | Ingest is open-world but HTTPS exact-allowlist only; probe is local | Immediate asset/probe object |
| Render, analysis, template apply, brand apply, batch, workflow | Validates expanded work, then enqueues bounded tenant media processing | Worker egress only when S3 is configured | Immediate `job_id`; poll job tools |
| Brand-kit upsert/delete | Mutates tenant-persisted Redis state | Local Redis only | Immediate mutation result |
| Drive/Discord export | Writes to an external provider | Disabled until enabled and exact destinations are allowlisted | Immediate provider ID after native confirmation |

Every runtime return is checked against its advertised closed output schema. Raw RQ arguments may remain in authenticated Redis until the bounded queue/result/failure TTL expires.

Every media stream is centrally checked for finite metadata, geometry, frame rate, audio layout, and decoded-work cost. The same policy revalidates ingested assets, every worker media input, and every persisted output. Planning gates reject oversized derived geometry, timed captions, lists, campaign cross-products, iterative renders, and nested template/workflow expansion before enqueue. FFmpeg and ffprobe then run with a fixed minimal environment, service-owned thread count, and Linux resource limits. These controls preserve the 55-tool contract in both authentication modes; they do not turn native codecs into a hostile-media sandbox.

Remote URL/Drive ingest reserves one full `MAX_INGEST_BYTES` charge against durable per-owner and service-wide staging admission before any HEAD or GET. The token-fenced lease spans download, probe, and persistence and is separate from managed-storage quota. Managed storage then reserves durable per-owner and service-wide asset count/bytes before persistence. Only committed, active, unexpired assets are visible; failed backend deletion is retried while retaining its quota charge. Worker execution also enforces per-job output count/bytes and S3 input-materialization bytes, and failed jobs submit committed partial outputs to the same deletion lifecycle. HTTP storage calls have bounded per-process admission and caller deadlines; a timed-out underlying thread can continue while holding its permit until late-result cleanup completes. `ffmpeg_capabilities` exposes the effective staging active, byte, lease, and heartbeat limits.

## Ingest and storage (6)

`media_ingest_from_url` and `media_ingest_from_drive` acquire durable staging admission before remote I/O, then fetch, validate, and persist one tenant asset through the separate durable asset quota; Drive ingest also requires its exact file ID allowlist. `media_probe` reads media and updates stored metadata. `media_get_download_url` is read-only and returns a short-lived signed URL that cannot outlive remaining asset retention. The two exports are opt-in external writes: `media_export_to_drive` requires `EXPORT TO GOOGLE DRIVE`, and `media_export_to_discord` requires `EXPORT TO DISCORD`.

- `media_ingest_from_url`
- `media_ingest_from_drive`
- `media_probe`
- `media_get_download_url`
- `media_export_to_drive`
- `media_export_to_discord`

## Core video (7)

All seven tools enqueue bounded local FFmpeg work and return a `job_id`; they do not accept raw shell commands or unrestricted FFmpeg arguments.

- `ffmpeg_transcode`
- `ffmpeg_thumbnail`
- `ffmpeg_trim`
- `video_add_text`
- `video_add_logo`
- `captions_burn_in`
- `video_concat`

## Analysis and QA (4)

`video_analyze` and `asset_compare` enqueue work. Rubric list/describe are immediate, read-only catalog operations.

- `video_analyze`
- `asset_compare`
- `rubric_list`
- `rubric_describe`

## Image to video (3)

All three tools enqueue bounded image/render work and return a `job_id`.

- `image_to_video`
- `images_to_slideshow`
- `images_to_slideshow_ken_burns`

## Audio (7)

All seven tools enqueue bounded audio work and return a `job_id`.

- `ffmpeg_extract_audio`
- `audio_normalize`
- `audio_mix`
- `audio_duck`
- `audio_mix_with_background`
- `audio_fade`
- `audio_trim_silence`

## Templates and brand kits (8)

Template list/describe and brand-kit get/list are read-only. Template/brand apply enqueue media work. `brand_kit_upsert` persists tenant configuration. `brand_kit_delete` is destructive, idempotent, and requires the exact phrase `DELETE BRAND KIT`.

- `template_list`
- `template_describe`
- `template_apply`
- `brand_kit_upsert`
- `brand_kit_get`
- `brand_kit_list`
- `brand_kit_delete`
- `brand_kit_apply`

## Batch and workflow (8)

All eight tools enqueue bounded multi-output or graph work. Batch sizes, workflow nodes, template layers, media counts, and per-job persisted output count/bytes are capped by configuration.

- `batch_export_formats`
- `batch_export_social_formats`
- `campaign_process`
- `render_social_ad`
- `render_testimonial_clip`
- `render_offer_card`
- `render_iterate`
- `workflow_run`

## Meta, presets, and jobs (12)

These are immediate read-only operations for local configuration, discovery, capability metadata, queue/job readback, and metrics. They do not invoke providers or media processing.

- `check_configuration`
- `list_capabilities`
- `get_endpoint_coverage`
- `get_tool_usage`
- `find_tools`
- `ffmpeg_list_presets`
- `ffmpeg_describe_preset`
- `ffmpeg_capabilities`
- `job_status`
- `job_progress`
- `job_logs`
- `metrics_snapshot`

## Agent workflow

1. Call `check_configuration` before depending on an optional integration.
2. Call `list_capabilities` or `find_tools` to narrow the catalog.
3. Call `get_tool_usage` for the exact input contract.
4. Ingest an asset and retain its returned asset identifier.
5. Invoke one bounded media operation.
6. Poll `job_status` or `job_progress`; inspect `job_logs` only for diagnosis.
7. Export the completed asset or create a short-lived signed download URL.

Rendering operations are asynchronous unless their descriptor says otherwise. Never infer completion from job submission alone.
