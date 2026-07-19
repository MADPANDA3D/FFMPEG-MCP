# Contributing

Thank you for improving FFMPEG MCP.

1. Open an issue for substantial behavior or contract changes.
2. Fork the repository and create a focused branch.
3. Keep credentials, customer data, private hostnames, and generated media out of commits.
4. Add or update offline tests for behavior changes.
5. Run `uv sync --frozen --group dev`, `uv run pytest`, and `uv run python scripts/check_source_safety.py`.
6. Open a pull request that explains the user-visible change, security impact, and verification performed.

Tool changes must preserve a deterministic catalog, clear input bounds, and both authentication modes. Changes that alter the tool count, tool names, release metadata, or deployment contract must update tests and documentation in the same pull request.

By contributing, you agree that your contribution is licensed under the repository's MIT License.
