# Conveyer — project instructions

## Python development: REPL-driven, always

For **any** Python work in this repo — writing, modifying, or debugging code —
use the `repl-driven-python` skill (linked at `.claude/skills/repl-driven-python/`,
shared from agent-dot-files; read its SKILL.md before writing the first line of
Python).

The non-negotiable rule it enforces: **write each function, validate it in the
live REPL (persistent Jupyter kernel, via
`.claude/skills/repl-driven-python/repl_client.py`, launched through the project
interpreter — `uv run python`) against real inputs and edge cases, and only then
integrate it into the module file.** Never write an unvalidated function directly
into a file, and never use "re-run the whole script" as the feedback loop.

## Data architecture

For design, modeling, and pipeline-architecture questions, use the
`rich-data-architect` skill (linked at `.claude/skills/rich-data-architect/`).
The skill carries the general principles; the conveyer-specific context below is
the project side of that consultation.

### The conveyer context

Conveyer is a **batch lane**: an immutable-fact pipeline on Iceberg/S3 executed
as Spark jobs (Glue / EMR Serverless), sharing one programming model with an
operational event lane (Event Sourcing + CQRS on MongoDB). It is a deliberate
instantiation of the skill's principles. When consulting on it, use its
vocabulary and defend its invariants:

| Conveyer construct | Principle it embodies |
|---|---|
| Append-only raw + fact tables (IAM-enforced, not convention) | facts are values; information accretion; immutability by construction, not discipline |
| Current state as disposable fold of facts | epochal model: derived state, rebuildable; DB-as-value via Iceberg time travel |
| Pure `apply` / `post_check` / `fold`; framework owns all I/O (CI-enforced, no `boto3`/`spark.read` in transforms) | pure functional core, effects at the edge; artifact-level enforcement over guardrails |
| Fixed stage sequence (land → pre_check → pull → apply → post_check → commit → fold → publish) | flow orientation; transform/move/remember kept separate; co-effects declared, not buried |
| `(batch_id, record_key, content_hash)` dedup + content-hash delta detection | idempotency as a load-bearing requirement; reruns are no-ops |
| Per-aggregate deterministic ordering inside the fold | ordering solved structurally, not by streaming middleware |
| `batch-started` / `batch-completed` on EventBridge gating consumers | reified process, broadcast novelty, batch-coherent perception |
| domain-id (aggregate-root id) shared across lanes | identity distinct from value; one identity, two runtimes |
| Quarantine with reasons, never silent drops | errors as data, not effects |
| Additive-only schema evolution; breaking change ⇒ new table | extensibility; the past doesn't change, so contracts over it can't either |
| Thin pipeline package (yaml + schemas + pure transforms + golden tests) | small interfaces; small implementation surface; easy fabrication (agents can write it) |

Standing guidance for conveyer consultations:

- **Defend the seams.** The most likely erosion vectors are: I/O sneaking into
  transforms ("just one lookup"), current-state tables quietly becoming a system
  of record someone writes to directly, consumers reading facts mid-batch instead
  of waiting for `batch-completed`, and "convenient" UPDATEs to fact tables during
  incident cleanup. Each is a complecting event; name it as such.
- **Routing rule between lanes** is a latency question only: sub-second downstream
  need → event lane; seconds-to-minutes tolerable → batch lane. Push back on
  forking the model (two taxonomies, two rule definitions) — the lanes are two
  runtimes of one model.
- **Fact granularity**: conveyer facts are typed records with `domain_id`,
  sequence/event-time, `fact_type`, lineage, payload — coarser than datoms, which
  is a reasonable trade for Spark/Iceberg economics. If someone needs
  attribute-level change tracking or bi-temporal queries, discuss the trade
  explicitly (business time as data on the fact/batch, technical time from the
  pipeline) rather than pretending the row-fact model gives it for free.
- **Known open items** you can help design when raised: quarantine remediation
  workflow (review queue, ownership, SLA); cross-lane contract governance (one
  authored source for business rules); cross-materialization inventory into
  `domainDB`; Glue→EMR placement thresholds; the pipeline generation spec for
  developers/agents.


<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:6cd5cc61 -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/SYNC_CONCEPTS.md for details and anti-patterns.

## Agent Context Profiles

The managed Beads block is task-tracking guidance, not permission to override repository, user, or orchestrator instructions.

- **Conservative (default)**: Use `bd` for task tracking. Do not run git commits, git pushes, or Dolt remote sync unless explicitly asked. At handoff, report changed files, validation, and suggested next commands.
- **Minimal**: Keep tool instruction files as pointers to `bd prime`; use the same conservative git policy unless active instructions say otherwise.
- **Team-maintainer**: Only when the repository explicitly opts in, agents may close beads, run quality gates, commit, and push as part of session close. A current "do not commit" or "do not push" instruction still wins.

## Session Completion

This protocol applies when ending a Beads implementation workflow. It is subordinate to explicit user, repository, and orchestrator instructions.

1. **File issues for remaining work** - Create beads for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **Handle git/sync by active profile**:
   ```bash
   # Conservative/minimal/default: report status and proposed commands; wait for approval.
   git status

   # Team-maintainer opt-in only, unless current instructions forbid it:
   git pull --rebase
   git push
   git status
   ```
5. **Hand off** - Summarize changes, validation, issue status, and any blocked sync/commit/push step

**Critical rules:**
- Explicit user or orchestrator instructions override this Beads block.
- Do not commit or push without clear authority from the active profile or the current user request.
- If a required sync or push is blocked, stop and report the exact command and error.
<!-- END BEADS INTEGRATION -->
