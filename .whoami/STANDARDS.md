# STANDARDS.md — Universal Technical Standards
> These are the non-negotiable technical standards for every project.
> They apply regardless of stack, client, or phase.
> Project-specific overrides go in AGENTS.md under "Project Rules."

---

## Version Control

Every commit is a semantic commit. No exceptions.

```
feat(scope):     new capability added
fix(scope):      bug corrected
refactor(scope): code restructured without behavior change
docs(scope):     documentation only
chore(scope):    dependencies, config, tooling
test(scope):     tests added or updated
build(scope):    build system or CI changes
```

Commit after every meaningful unit of work. Never batch unrelated changes. Never commit broken code. If Linear is active, include the issue key in the commit body: `Refs: [KEY-123]`.

Branch strategy: `main` is always deployable. Feature work goes on branches named `feat/[description]` or `fix/[description]`. Merge via PR with a brief description of what changed and how it was tested.

---

## Linear Issue Structure

Every non-trivial task needs a Linear issue before coding starts. A properly structured issue has:

| Field | Requirement |
|---|---|
| **Title** | `Area: action + outcome` — e.g., `API: add rate limiting to auth endpoint` |
| **Description** | Context, acceptance criteria, any constraints |
| **Labels** | At least one area label (`API`, `Web`, `Infra`, `Docs`, `Worker`) |
| **Assignee** | Set before moving to In Progress |
| **Priority** | Set based on impact and urgency |
| **Milestone** | Assign to the current milestone/cycle |

Status flow: `Backlog → In Progress → In Review → Done`

Close with an outcome note: what was done, how it was verified, any follow-up issues created.

---

## Repository Structure

```
project-root/
├── .whoami/            ← Agent memory system (this directory)
├── AGENTS.md           ← Agent entry point
├── HANDOVER.md         ← Session handover file
├── README.md           ← Human-readable project overview
├── docs/               ← Specs, architecture docs, brand docs, working notes
│   ├── brand-core.md   ← Brand copy and proof points (if applicable)
│   └── agent-working-memory.md  ← Long-form audit notes (if applicable)
├── .env.example        ← All required env vars documented (no values)
├── .env                ← Actual values (NEVER committed)
└── [source code]       ← src/, client/, server/, apps/ per framework
```

Keep the root clean. If a file doesn't fit a clear category, it probably belongs in `docs/`.

---

## Environment Variables

Every project must have an `.env.example` that documents every required variable with a comment explaining what it is. Format:

```bash
# Required — your database connection string
DATABASE_URL=

# Required — JWT signing secret (generate with: openssl rand -base64 32)
JWT_SECRET=

# Optional — enables debug logging when set to "true"
DEBUG=
```

Update `.env.example` every time a new variable is added. Never commit `.env`. Never log secret values.

---

## TypeScript Projects

Strict mode always: `"strict": true` in `tsconfig.json`. No `any` — use `unknown` and narrow. Explicit return types on exported functions. Zod for all runtime validation at API boundaries. Run `tsc --noEmit` before every commit.

Standard stack: React 19 + Vite + TypeScript + Tailwind CSS 4 + shadcn/ui + Framer Motion + Wouter + Sonner. Use `pnpm` as the package manager. Use `pnpm commit` (commitizen) for guided semantic commits when unsure.

File naming: `PascalCase` for React components, `camelCase` for hooks and utilities, `kebab-case` for directories and non-component files.

Never nest `<a>` inside a router `<Link>` component. Use Sonner for toasts — not react-toastify. Use Lucide React for icons.

Tailwind CSS 4 specifics: use OKLCH color format in `@theme inline` blocks. For `clip-path` diagonal sections, add negative margin-top to compensate for the clipped area.

---

## Python Projects

Python 3.12+. Type hints on all functions. 4-space indentation. `snake_case` for functions and variables, `PascalCase` for classes, `UPPER_SNAKE_CASE` for constants.

Toolchain: `uv` for dependency management (`uv sync --group dev`), `ruff format` + `ruff lint` for formatting and linting, `isort --profile black` for import sorting, `mypy` for type checking. Run all four before committing.

Tests: `pytest` with `pytest-asyncio` (`asyncio_mode=auto`). Test files named `test_*.py`. Keep critical paths covered.

---

## Docker Projects

After any change that affects runtime behavior — source code, environment variables, configuration — rebuild and restart the affected containers immediately. Do not wait for a prompt.

```bash
docker-compose -f [compose-file] up -d --build [service-name]
docker-compose -f [compose-file] ps   # validate immediately after
```

If only one service changed, restart only that service. If cross-service contracts changed, rebuild all impacted services in the same pass. Always validate with `ps` and report status.

---

## Security

Never commit secrets. Never log secret values. Validate all required environment variables at application startup — fail fast with a clear error message listing what is missing. Store secrets in `.env` locally and in the deployment platform's secret manager in production. Follow the principle of least privilege for all service accounts and API keys.

---

## Testing

Write tests for critical paths, not everything. A test that verifies the happy path and the main failure mode is better than no tests. Mock external services in tests — never hit production APIs. Name test files to match the file they test: `McpCard.test.tsx` tests `McpCard.tsx`.

If no test suite exists, note it in MEMORY.md and flag it as technical debt. Do not block shipping on test coverage, but do not pretend the debt doesn't exist.

---

## Code Review Checklist (Before Marking PR Ready)

```
[ ] Does the code do what the Linear issue says it should do?
[ ] Are there any obvious edge cases not handled?
[ ] Are new environment variables documented in .env.example?
[ ] Does the README need updating?
[ ] Are there any hardcoded values that should be env vars?
[ ] Does this change break any existing behavior?
[ ] Are there TypeScript errors or lint warnings?
[ ] Is the commit history clean and semantic?
```
