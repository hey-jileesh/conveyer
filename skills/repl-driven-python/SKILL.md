---
name: repl-driven-python
description: >-
  REPL-driven development workflow for writing Python code. Use this skill
  whenever writing, modifying, or debugging Python code in this project —
  before writing any new function into a file. It mandates the Clojure-style
  loop: draft each function, validate it in a live persistent REPL (Jupyter
  kernel) against real inputs and edge cases, and only then integrate it into
  the module file. Also use it when debugging Python (reproduce in the REPL
  first), when a script re-runs expensive setup on every invocation, or when
  the user asks for "REPL", "interactive", or "kernel" workflows.
---

# REPL-Driven Python Development

Work the way a Clojure developer works at the REPL: never integrate a
function into a file until it has been evaluated against real inputs in a
live process. Files are the *record* of what worked; the REPL is where the
work happens.

## The prime directive

**Write function → validate in REPL → only then integrate into the file.**

Never write a new or modified function directly into a module and assume it
works. Every function must be defined and exercised in the live kernel —
happy path, at least one edge case, and the failure mode you expect it to
guard against — before it is written into a `.py` file. If validation
surfaces a problem, fix it in the REPL and re-validate; the file only ever
receives functions that have already run.

This inverts the default LLM habit (write whole file → run whole file →
read traceback → edit → repeat). That loop re-executes expensive setup on
every iteration and debugs at the granularity of a file. The REPL loop
debugs at the granularity of one expression against state that is already
loaded, so each cycle costs seconds.

## Setup (once per session)

The kernel outlives each shell call, so state persists across your
invocations. `repl_client.py` lives alongside this skill.

```bash
pip install jupyter_client ipykernel --break-system-packages -q  # if missing
python <path-to-skill>/repl_client.py start
```

`start` enables `%autoreload 2` automatically: once a module is imported in
the kernel, later edits to its file take effect on the next call — no
restart, no re-import, state preserved.

Evaluate code:

```bash
python repl_client.py eval "df.shape"
python repl_client.py eval -f snippet.py     # longer snippets from a file
echo "long code here" | python repl_client.py eval
```

Exit code is 0 on success, 1 if the code raised — check it. `stop` shuts
the kernel down; `start` on a live kernel is a no-op.

## The loop

1. **Load context once.** Import modules and load expensive state (dataset,
   connection, config) into the kernel at the start. Never reload it per
   iteration — keeping it warm is the entire payoff.

   ```bash
   python repl_client.py eval "import pandas as pd; df = pd.read_parquet('lane.parquet'); df.shape"
   ```

2. **Draft the function in the REPL, not the file.** Define it directly in
   the kernel (via `eval -f` with a scratch snippet file for anything
   multi-line).

3. **Validate it.** Call it on real loaded data and on edge cases: empty
   input, None, wrong dtype, boundary values — whatever this function must
   survive. Inspect actual outputs, don't reason about hypothetical ones.
   A function is validated when you have *seen* it return the right value
   for representative and adversarial inputs.

4. **Integrate.** Only now write/edit the function into its module file,
   exactly as validated.

5. **Confirm the integrated version.** Because of autoreload, the kernel
   picks up the file edit; call the function *through the module*
   (`mymod.fn(...)`) to prove the file version — not just the REPL draft —
   is correct.

6. **Pin it with a test.** Convert what you learned in steps 3–5 into a
   pytest case so the knowledge outlives the kernel. REPL validation is for
   iteration speed; tests are the durable record.

## Debugging

Reproduce the failure in the kernel *first* — with real state already
loaded, shrinking a failing case to a one-line expression is fast. Then fix
in the REPL, verify, integrate, and re-verify through the module, same as
above. Never fix a bug by editing the file and re-running a whole script as
the test of the fix.

## Autoreload caveats (know when to restart)

`%autoreload 2` handles function and method bodies well. It does **not**
retroactively fix: existing *instances* of a changed class, changed
decorators already applied, module-level constants already computed, or
`from mod import fn` bindings (prefer `import mod` + `mod.fn` in the kernel
for exactly this reason). If behavior seems stale after an edit of that
kind, don't chase ghosts — `stop`, `start`, and rebuild state; it costs one
context load.

## Rules

- No new/changed function reaches a file without having run in the kernel.
- Prefer small, pure functions — they are the unit of REPL validation.
  Isolate I/O at the edges so the core logic is testable on plain values.
- In the kernel, `import module` (not `from module import fn`) so
  autoreload works.
- Check `eval`'s exit code; a non-zero exit means the snippet raised.
- One kernel per task. If state becomes confusing, restart rather than
  reasoning about a polluted environment.
- Finish every piece of work by (a) confirming the file version through the
  module and (b) writing the pytest case.
