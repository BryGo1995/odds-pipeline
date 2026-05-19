# Generate-Diagram Skill Redesign

**Date:** 2026-05-19
**Status:** Approved
**Scope:** Replace `~/.claude/commands/generate-diagram.md` with an MCP-native version. Global skill, not a project artifact — this spec lives in the odds-pipeline repo for convenience but the skill itself is user-global.

---

## Motivation

The current skill is a Mermaid → custom-converter → JSON-patch pipeline that does not use the Excalidraw MCP server. Most of its rules (8-node cap, no subgraphs, no decision shapes, 2–3 word edge labels, strict ID regex) exist to work around the converter, not to make better diagrams. Output is flat and uniform regardless of subject, and the skill auto-commits both files at the end of every run.

The Excalidraw MCP now exposes `read_me`, `create_view`, and `export_to_excalidraw`. The model can emit Excalidraw element JSON directly, render in-session, and access a color palette — eliminating the converter, the post-process hack, and most of the syntactic restrictions.

---

## Design

### Skill flow

The rewritten `~/.claude/commands/generate-diagram.md` walks through:

1. **Parse `$ARGUMENTS`** for an optional diagram name (e.g. `/generate-diagram mlb-ingest`). If absent, name is inferred from the scope answer in step 2.
2. **Ask scope** via a single `AskUserQuestion`:
   - *What subsystem or flow to diagram?* (free-text or pick from suggestions inferred from the codebase)
   - *Level of detail:* overview / detailed / single-path trace
3. **Read targeted code** matching the scope — entry points, DAGs, clients, models. Not the full repo.
4. **Check for existing diagram** at `docs/diagrams/<name>.excalidraw`. If present, load it and treat current state as the starting point.
5. **Call `read_me`** to load the Excalidraw element format and palette reference.
6. **Compose elements** following the style guidance below.
7. **Render via `create_view`** for in-session feedback (browser draw-on animation).
8. **Write JSON** to `docs/diagrams/<name>.excalidraw` (mkdir -p first).
9. **Report** the file path and a one-line summary of what was drawn. No commit, no upload.

### Style & structure guidance (baked into the skill body)

This is the quality lever. The skill file teaches the model what good looks like:

**Layered zones, top-to-bottom by data flow.** Every diagram organizes nodes into horizontal bands:
- *Sources* — external APIs, triggers, schedulers, manual inputs
- *Orchestration* — DAGs, schedulers, controllers
- *Processing* — transformers, ML scoring, business logic
- *Storage* — datastores, caches, object stores
- *Sinks* — outputs, notifications, downstream consumers

Not every diagram has all five; pick what applies. Align nodes in rows so the band structure is visually obvious.

**Color coding by role.** Use the MCP palette from `read_me` to assign one color per role: external systems, orchestration, processing, storage (cylinder shape), sinks. Consistent within a diagram, not arbitrary.

**Edge labels are short verb phrases**, not 2–3 word fragments: "writes scored predictions" beats "writes". "Triggers nightly at 02:00" beats "triggers".

**Size by scope, not by hard cap.**
- *Overview* → ~5–8 nodes, top-level components only
- *Detailed* → 10–20 nodes, includes transformations and intermediate stores
- *Single-path trace* → linear chain, every step in the path

Guidance line: "fit comfortably on one screen at the chosen scope; if it overflows, the scope was too broad."

**Omit by default**: helper modules, logging, config loaders, language-internal utilities. Include only if load-bearing in the flow being drawn.

**Group with containers** where a zone has 3+ nodes — use a rectangle behind them with a low-opacity fill and a zone label.

### MCP call mechanics

**MCP calls used, in order within the flow above:**

- `read_me` (flow step 5) — called every run. Output is small; no need to cache across runs.
- `create_view` (flow step 7) — full element array. Streams elements with draw-on animation in the user's browser.
- *(not called)* `save_checkpoint`, `read_checkpoint`, `export_to_excalidraw`.

**Element composition (flow step 6):** rectangles for services, cylinders for datastores, arrows with text labels for edges, optional containers for zone grouping. Role→color mapping is consistent within a diagram; specific palette IDs come from `read_me`.

**Refine-in-place.** When the target file already exists at step 4 of the flow:
- Read the file as JSON.
- Treat its current node/edge set as the starting point. Preserve positions and any hand-edits the user made (e.g. by drag-and-dropping the file into excalidraw.com).
- The scope answer defines the *delta* ("add the X service", "redraw the storage band", "simplify orchestration").
- Modify only what the scope answer touches; leave untouched zones intact.
- Re-render the full updated state via `create_view`.
- Save the merged JSON back to the same path.

**No MCP checkpoint usage.** The local `.excalidraw` file is the single source of truth. `save_checkpoint` / `read_checkpoint` are not used — they would be a second source of truth and would require the MCP server to retain state across sessions.

**No upload by default.** `export_to_excalidraw` is not called. If the user explicitly asks for a shareable link in a follow-up turn, the skill calls it then.

### Error handling

| Case | Behavior |
|---|---|
| `read_me` fails (MCP down/unregistered) | Report failure and stop. No Mermaid fallback. |
| `create_view` rejects JSON | Retry once after re-reading `read_me` and tightening the element shape. If retry fails, print the offending element and stop. Do not write bad JSON to disk. |
| `docs/diagrams/` missing | `mkdir -p docs/diagrams` before saving. |
| Existing file, mismatched scope | If existing `<name>.excalidraw` clearly covers a different system than the scope answer, ask once: refine, or pick a new name? |
| Scope answer too broad | If the answer would produce a 40-node mess (e.g. "the whole project" at detailed level), ask one follow-up: narrow to a subsystem, or stay overview-level? Single probe, no looping. |

### What is removed from the current skill

- The entire Mermaid path: no `.mmd` file written, no `flowchart TD` syntax rules.
- `~/.claude/scripts/mmd-to-excalidraw.js` is not invoked. (The script can stay on disk but the skill no longer depends on it.)
- The inline Node post-processor that rewrites `label` → `text` elements.
- The 8-node cap, no-subgraphs rule, no-decision-shapes rule, `[A-Za-z0-9]+` ID restriction, label character restrictions, 2–3 word edge label cap.
- The final `git add` / `git commit` step.

### Verification

No automated tests — this is a markdown command file. Verification is two manual runs:

1. **Fresh diagram:** `/generate-diagram mlb-ingest` from clean state. Confirm file written at `docs/diagrams/mlb-ingest.excalidraw`, in-session render appears, zones/colors applied.
2. **Refine:** re-run with a delta scope answer (e.g. "add the new feature store"). Confirm prior nodes preserved and only the affected zone changed.

The skill itself cannot validate quality — that is the user's judgment call relative to the old pipeline.

---

## Out of scope

- A `--review` flag for two-stage design-then-draw workflow (considered as approach C during brainstorming; deferred unless needed).
- Migrating existing `.mmd` files in `docs/diagrams/` to `.excalidraw`. The new skill produces `.excalidraw` only; old `.mmd` files remain untouched.
- Removing the obsolete `~/.claude/scripts/mmd-to-excalidraw.js` script. Can be deleted later once no skill references it.
