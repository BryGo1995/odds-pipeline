# Excalidraw Diagram Generation System — Design Spec

**Date:** 2026-03-18
**Status:** Approved

---

## Overview

A reusable, project-agnostic system for generating Excalidraw architecture diagrams from codebases. Claude analyzes the project structure and produces a Mermaid diagram, which a Node.js script converts to a `.excalidraw` file. Diagrams are stored per-project in `docs/diagrams/` and viewed in the browser.

---

## Architecture

Three components, all installed globally in `~/.claude/`:

### 1. `generate-diagram` Skill

- Invoked via `/generate-diagram [name]`
- If no name is given, Claude infers one (e.g., `architecture`, `data-flow`)
- Claude reads the codebase (glob structure, key files) and produces a Mermaid `flowchart TD` diagram
- Writes the Mermaid source to `docs/diagrams/<name>.mmd`
- Runs the converter script: `node ~/.claude/scripts/mmd-to-excalidraw.js docs/diagrams/<name>.mmd`
- Reports the output path to the user

### 2. `mmd-to-excalidraw.js` Script

- A Node.js wrapper around `@excalidraw/mermaid-to-excalidraw`
- Accepts a `.mmd` file path as its only argument
- Writes a `.excalidraw` file to the same directory with the same base name
- Installed once at `~/.claude/scripts/mmd-to-excalidraw.js`

### 3. Brainstorming + Planning Hooks

- **Brainstorming**: after design approval (step 5), `/generate-diagram architecture` runs automatically before the spec doc is written, producing a visual artifact alongside the spec
- **Planning**: each significant milestone in a plan includes a reminder to run `/generate-diagram` to update the diagram; triggering is manual, not automatic

---

## Diagram Content

Claude decides what to capture based on codebase analysis. Examples:

- **Data pipeline projects**: DAGs → transformers → database → external APIs
- **Django projects**: models → views → serializers → external services
- **General**: modules, services, data flows, key dependencies

---

## File Layout (per project)

```
docs/
  diagrams/
    architecture.mmd        # Mermaid source (version-controlled)
    architecture.excalidraw # Generated output (version-controlled)
```

Both files are committed so diagrams are browsable without regenerating.

---

## Installation (one-time, global)

```bash
# 1. Install the npm converter
npm install -g @excalidraw/mermaid-to-excalidraw

# 2. Place the wrapper script
cp mmd-to-excalidraw.js ~/.claude/scripts/

# 3. Install the skill
# Place generate-diagram skill in ~/.claude/plugins/
```

No per-project configuration required.

---

## Workflow

```
/generate-diagram [name]
  → Claude globs project structure and reads key files
  → Writes docs/diagrams/<name>.mmd
  → Runs node ~/.claude/scripts/mmd-to-excalidraw.js docs/diagrams/<name>.mmd
  → Outputs docs/diagrams/<name>.excalidraw
  → User opens .excalidraw file at excalidraw.com to view/edit
```

---

## Constraints

- Requires Node.js to be installed
- Viewing requires a browser (excalidraw.com or self-hosted)
- Primary editor is Neovim — no IDE plugin involved
- No per-project setup; all tooling is global in `~/.claude/`
