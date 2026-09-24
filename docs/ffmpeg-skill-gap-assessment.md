# kajisho5/ffmpeg-skill gap assessment

Reference reviewed: `kajisho5/ffmpeg-skill` at commit
`49cf7b1ffd6d51767b72e292cb9275a55b197330` from the runner-provided,
read-only checkout. The MIT reference was treated as design evidence; no source
was copied.

| Reference idea | Decision | FFmpeg MCP implementation |
|---|---|---|
| Probe-first, structured contracts | Adopted | Existing `media_probe` remains the source-inspection primitive; the new Reel validator returns structured errors and a deterministic input fingerprint. |
| Project-based whole-edit rendering | Adapted | A bounded `madpanda_vertical_reel` job assembles ordered durable clip IDs and trims, timed overlays, one narration take, shaped music, and a versioned outro without accepting arbitrary FFmpeg commands. |
| Join, overlay, graphics, audio, export | Retained | Existing tools already cover these operations and remain backward compatible. The approved shaped audio values are encapsulated in the Reel contract rather than exposing arbitrary filter graphs. |
| Dry-run planning and fingerprints | Adopted | `madpanda_reel_validate` performs no render and returns a SHA-256 input fingerprint. |
| Contact-sheet verification | Adopted | Every successful Reel job creates a six-frame contact sheet and signed preview URL. Delivery still requires an agent to inspect it; the service does not claim subjective visual approval. |
| Platform delivery checks | Adopted | The job must fully decode and verify 1080x1920, 24 fps, H.264/yuv420p, AAC stereo/48 kHz, fast-start, -14 LUFS, -1.5 dBTP, LRA 10, 30-45 seconds, output hashes, and unchanged input hashes before publishing signed URLs. |
| `brand.json` / `project.json` files | Adapted | Brand kits stay in the existing Redis contract. Versioned templates and Reel plans are JSON objects managed through Portal tools, avoiding filesystem paths and arbitrary code. |
| Shell-command/project execution | Rejected | Arbitrary FFmpeg command execution and local-path access violate the Portal-only, preset-bound safety model. |
| Replacement public MCP | Rejected | MAD MCP Portal remains the only intended agent access surface. |

## New Portal-native workflow

1. Call `media_upload_begin`, PUT bytes with the declared MIME and SHA-256
   headers, then call `media_upload_complete`.
2. Search and curate reusable originals with the `clip_library_*` tools.
3. Create immutable template versions with `template_version_upsert`.
4. Read `madpanda_reel_contract`, validate the complete plan with
   `madpanda_reel_validate`, and queue it with `madpanda_reel_render`.
5. Poll `job_status`; a successful result contains measured delivery QA plus
   signed URLs for the master and generated contact sheet.

Direct completion deduplicates by SHA-256, preserves originals, and exposes
privacy-safe metadata only. Archiving hides a clip from normal search without
deleting media.
