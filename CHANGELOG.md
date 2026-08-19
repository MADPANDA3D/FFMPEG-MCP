# Changelog

All notable changes are documented here. This project follows Semantic Versioning.

## [Unreleased]

## [1.0.1] - 2026-08-19

- Updated the deterministic dependency locks to patched `cryptography 50.0.0`.
- Added a frozen Standard v1.2 regression for the complete 55-tool compatibility contract.
- Serialized fresh-volume initialization between the server and worker across all Compose variants.
- Aligned package, module, image, Compose, environment, and CI release metadata on `1.0.1`.

## [1.0.0] - 2026-07-18

- Established the public package, container, and release contract.
- Added standalone bearer-token and Portal grant-token deployment modes.
- Published a documented catalog of 55 bounded media tools.
- Added deterministic Python dependencies, hardened containers, provenance checks, and offline CI gates.
- Added a durable token-fenced asset lifecycle with atomic owner/global count-and-byte quotas, active/unexpired visibility, retry-safe deletion, retention-bounded signed URLs, per-job output/materialization budgets with partial-output cleanup, per-process bounded ASGI storage execution, and symlink-safe age-bounded orphan staging cleanup. Legacy runtime asset records require an intentional reset or offline migration before admission.
- Added atomic tenant/global job admission with worker lease refresh, terminal cleanup, ambiguity-safe enqueue reconciliation, and a binary-safe RQ Redis client; bounded tenant metrics and brand-kit storage; and fail-closed Redis `noeviction` controls with AOF/COW memory headroom.
- Added central all-stream probe and decoded-work gates at ingest, worker input, and output persistence; finite pre-enqueue geometry, list, workflow, render, caption, and cross-product budgets; sanitized FFmpeg/ffprobe environments; Linux address-space, CPU, descriptor, core, and output limits; bounded FFmpeg threads; and accurate nullable job-readback schemas, without changing the 55-tool or dual-auth contract.
- Added atomic owner/global remote-ingest staging count and byte admission before network I/O, conservative maximum-size charges, cancellation-safe acquisition reconciliation, token-fenced crash leases and heartbeats, probe/storage settlement finalizers, fail-closed Redis behavior, runtime capability readback, and real-Redis contract smoke coverage; catalog `2026-07-18.4` preserves the 55-tool and dual-auth contract.

[Unreleased]: https://github.com/MADPANDA3D/FFMPEG-MCP/compare/v1.0.1...HEAD
[1.0.1]: https://github.com/MADPANDA3D/FFMPEG-MCP/releases/tag/v1.0.1
[1.0.0]: https://github.com/MADPANDA3D/FFMPEG-MCP/releases/tag/v1.0.0
