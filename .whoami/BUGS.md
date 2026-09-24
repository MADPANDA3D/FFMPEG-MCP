# BUGS.md — Project Bug Memory
> Append-only — never delete entries. Mark resolved bugs with [RESOLVED — DATE].
> Check this file before starting any task. The bug you're about to hit may already be documented.
> If this file is empty, that's fine — it fills up as the project grows.

---

## How to Use This File

Before writing any code, scan this file for patterns relevant to what you're about to do. If you encounter a new bug during a session, append it immediately — even if you fix it in the same session. The goal is a library of recurring failures and operational traps for this specific MCP.

---

## Bug Entry Format

```markdown
### [DATE] — Short description of the bug
**Status:** [OPEN] / [RESOLVED — DATE] / [WORKAROUND]
**Symptom:** What the developer or user sees
**Root cause:** Why it happens
**Fix:** What was done to resolve it
**Prevention:** How to avoid this in the future
```

---

## Bug Log

<!-- Agent: append new bugs below this line as they are discovered -->

### 2026-09-23 — New Python files unreadable in non-root runtime
**Status:** [RESOLVED — 2026-09-23]
**Symptom:** The containerized unittest discovery failed to import `reel_library.py` and `tests/test_reel_library.py` with `PermissionError`.
**Root cause:** The cycle-1 files were created with mode `0600`, but the API and worker containers run under separate non-root runtime UIDs.
**Fix:** Changed repository source, test, and documentation files added by TKT-000470 to mode `0644`.
**Prevention:** Check newly added file modes before handing source to the host-owned container validation gate.

### 2026-09-23 — New registry tools missing ToolManifest output schemas
**Status:** [RESOLVED — 2026-09-23]
**Symptom:** Containerized candidate validation failed while building the manifest with `ValueError: Tool metadata has no output schema: clip_library_archive`.
**Root cause:** The 12 new Reel workflow tools were added to registry metadata without corresponding branches in `_output_schema`.
**Fix:** Added explicit output schemas matching every new handler's privacy-safe response shape and verified all 67 descriptors build.
**Prevention:** Any registry addition must add its input/output contract in the same change and pass a complete manifest-build smoke.

### 2026-09-24 — Background mix did not preserve an audible voice branch
**Status:** [RESOLVED — 2026-09-24]
**Symptom:** `audio_mix_with_background` could report success for an output containing the music bed but no audible voice.
**Root cause:** The ducking graph reused one voice label as both the sidechain input and final mix input instead of explicitly splitting it into two FFmpeg branches; the wrapper also accepted `voice_gain` without applying it in ducking mode.
**Fix:** Added an explicit `asplit`, applied `voice_gain` to the audible branch, and versioned affected cache keys to prevent reuse of pre-fix outputs.
**Prevention:** Execute media filtergraphs with synthetic fixtures that prove every stream has a dedicated path to the final output.
