"""Ownership grep test -- LLD §8: "The golden suite asserts the ownership
rule by grepping call sites of `ledger.append` / `emit` (cheap, honest)."

`registration/registrar.py`'s module docstring (§8, §8.5) is explicit that
its three functions are "the only code anywhere (besides §9.4's
reconciliation, ...) that performs ledger/event effects" for the
REGISTRATION path. Two M5 beads add their own narrow, LLD-mandated
exceptions on top of that baseline (each is the ONLY thing that changed in
`_ALLOWED_CALLERS`, not a broader carve-out):

* `maintenance/optimize.py` -- §9.4's supersession-reconciliation
  `fx.ledger.append` (the exception the paragraph above already names).
* `absence/detector.py` -- §9.3's own normative pseudocode calls
  `fx.emit("delivery-overdue", ...)` directly from the absence sweep (not
  through `registrar.py`, which the sweep has no reason to call); this is a
  DIFFERENT event (`delivery-overdue`, not `delivery-registered`) on a
  disjoint path, not registrar's job creeping elsewhere.

A plain `ast` walk (not a text regex) over every `ingestion/**/*.py` file,
looking for calls shaped like `<expr>.ledger.append(...)` or
`<expr>.emit(...)` -- matching the two `Effects` capabilities LLD §8.5's
`execute` interpreter uses (`fx.ledger.append`, `fx.emit`). Any call site
outside the allowlist is a violation: some other module has started doing
registrar's job.
"""

from __future__ import annotations

import ast
from pathlib import Path

_PACKAGE_ROOT = Path(__file__).resolve().parent.parent.parent
_INGESTION_ROOT = _PACKAGE_ROOT / "ingestion"

# (relative POSIX path) -- the only files permitted to call `ledger.append`
# or `emit`, beyond `registration/registrar.py` itself: `maintenance/
# optimize.py` (§9.4's reconciliation append) and `absence/detector.py`
# (§9.3's direct `delivery-overdue` emit). Do not add a third entry without
# a matching LLD citation -- see the module docstring above.
_ALLOWED_CALLERS: frozenset[str] = frozenset(
    {
        "ingestion/registration/registrar.py",
        "ingestion/maintenance/optimize.py",
        "ingestion/absence/detector.py",
    }
)


def _terminal_attr(node: ast.expr) -> str | None:
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _is_ledger_append_or_emit_call(node: ast.Call) -> bool:
    func = node.func
    if not isinstance(func, ast.Attribute):
        return False
    if func.attr == "emit":
        return True
    if func.attr == "append":
        base = func.value
        return isinstance(base, ast.Attribute) and base.attr == "ledger"
    return False


def _call_sites(path: Path) -> list[int]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and _is_ledger_append_or_emit_call(node)
    ]


def test_only_registrar_calls_ledger_append_or_emit() -> None:
    violations: list[str] = []
    for path in sorted(_INGESTION_ROOT.rglob("*.py")):
        rel_path = path.relative_to(_PACKAGE_ROOT).as_posix()
        if rel_path in _ALLOWED_CALLERS:
            continue
        for lineno in _call_sites(path):
            violations.append(f"{rel_path}:{lineno}")

    assert violations == [], (
        "ledger.append/emit called outside registration/registrar.py: " + ", ".join(violations)
    )


def test_allowlisted_caller_actually_calls_both() -> None:
    """A cheap sanity check that the allowlist isn't vacuous -- registrar.py
    really does call both `ledger.append` and `emit` somewhere.
    """
    path = _PACKAGE_ROOT / "ingestion/registration/registrar.py"
    assert len(_call_sites(path)) >= 2
