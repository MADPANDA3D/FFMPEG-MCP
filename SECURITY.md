# Security policy

## Supported versions

Security fixes are made for the latest stable release. Upgrade to the newest release before reporting an issue against an older version.

## Report a vulnerability

Use GitHub's **Security** tab and choose **Report a vulnerability**. Do not open a public issue and do not include credentials, private media, tokens, or production logs in a report.

Include the affected version or image digest, a minimal reproduction, impact, and any suggested mitigation. You should receive an acknowledgement within five business days.

## Deployment responsibility

The server is not safe for unauthenticated Internet exposure. Configure exactly one supported mode, use a high-entropy access token, terminate TLS at a trusted reverse proxy, restrict allowed hosts and origins, and keep Redis on an internal network. URL ingest expands the trust boundary: keep the domain allowlist narrow and review resource limits before handling untrusted media.

See [docs/security-model.md](docs/security-model.md) for the boundary and operator checklist.
