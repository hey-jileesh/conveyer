"""Purity + idiom linter — LLD §12.2, enforcing the §7.0/D-17 functional idiom.

A stdlib `ast` walk over `ingestion/**/*.py` and `sources/**/*.py` (relative
to this module's parent directory, the `conveyer-ingestion` package root).
Two rule groups:

* PURITY — applies to `ingestion/core/**` and `sources/**` only (effects and
  entrypoints are exempt): banned imports of anything that could perform
  I/O or introduce nondeterminism, banned calls to the same effect, and a
  ban on `raise`/`try` (defects are values, per §7.0 rule 4) — with two
  narrow, hardcoded exemptions resolving the §6.1/§12.2 contradiction (a
  pydantic validator MUST raise `ValueError` to signal failure; see
  `conveyer-4ot.24`): `raise` inside a `@field_validator`/`@model_validator`
  method is exempt from `purity-raise`, and exactly two hardcoded
  `(file, function)` pairs are exempt from both `purity-try` and
  `purity-raise` entirely. See the rule tables below for the exact scope.
* IDIOM — applies to ALL of `ingestion/**` and `sources/**`: `class`
  statements are banned unless they are `@dataclass(frozen=True)`, a
  pydantic `BaseModel` subclass, or an `enum.Enum` subclass, EXCEPT for one
  narrow, hardcoded `(file, class name)` exemption resolving the
  §7.3/§12.2 contradiction (`class TransientError(Exception)` is "the only
  exception type in the codebase," §7.3; see `conveyer-4ot.26`) —
  `ingestion/effects/records.py::TransientError` only, not a general
  Exception-subclass exemption. Also banned everywhere: a small list of
  FP-framework/mocking imports.

This file and its own tests live under `tools/` and `tests/`, which are
*not* part of the walked tree — the linter is exempt from its own rules by
construction (§12.2 only names `ingestion/**` and `sources/**`), but it is
still written in the same functional style: plain values (`Violation`) and
plain functions, no class beyond that one frozen dataclass.
"""

from __future__ import annotations

import ast
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

# --- rule tables -----------------------------------------------------------

_PURITY_BANNED_IMPORT_ROOTS: tuple[str, ...] = (
    "boto3",
    "botocore",
    "pyiceberg",
    "paramiko",
    "requests",
    "urllib",
    "http",
    "socket",
    "subprocess",
    "os",
    "sys",
    "io",
    "pathlib",
    "sqlite3",
    "random",
    "tempfile",
    "threading",
    "multiprocessing",
    "time",
)

_IDIOM_BANNED_IMPORT_ROOTS: tuple[str, ...] = (
    "toolz",
    "cytoolz",
    "returns",
    "pyrsistent",
    "funcy",
    "effect",
)

# Banned as an exact dotted path, not a whole root: `unittest` itself (e.g.
# `unittest.TestCase`) is fine, only the `mock` submodule is banned.
_IDIOM_BANNED_IMPORT_DOTTED: tuple[str, ...] = ("unittest.mock",)

_BANNED_BARE_CALLS: frozenset[str] = frozenset({"open", "eval", "exec", "__import__"})

_BANNED_ATTR_CALLS: frozenset[tuple[str, str]] = frozenset(
    {
        ("datetime", "now"),
        ("datetime", "utcnow"),
        ("datetime", "today"),
        ("date", "today"),
        ("uuid", "uuid1"),
        ("uuid", "uuid4"),
        # uuid.uuid5 is deliberately absent — deterministic, "now" is a parameter.
    }
)

_ALLOWED_ENUM_BASE_NAMES: frozenset[str] = frozenset(
    {"Enum", "IntEnum", "StrEnum", "Flag", "IntFlag"}
)

# Director adjudication of the LLD §6.1-vs-§12.2 contradiction (conveyer-4ot.24):
# §6.1 mandates `@field_validator`/`@model_validator` methods that
# `raise ValueError(...)` — pydantic v2 has no non-raising failure protocol
# for custom validators — while §12.2/§7.0 rule 4 bans `raise`/`try` outright
# in `core/`. Resolved by two independent, narrow mechanisms (see
# `_validator_decorated_raise_ids` and `_control_flow_violations` below):
#
# 1. `raise` (only) inside a function decorated with `field_validator` or
#    `model_validator` (bare, attribute, or call form; classmethod-paired or
#    not) is exempt from `purity-raise`. `try` is NOT exempted by this rule —
#    it stays banned inside validator bodies.
# 2. Exactly TWO hardcoded `(file, function)` pairs are exempt from BOTH
#    `purity-try` and `purity-raise` entirely — identified by (file,
#    function), not by line number, and not via a config/pragma mechanism
#    (none exists; do not add one):
#    - `ingestion/core/completeness.py::parse_manifest` (§7.3: "the ONE place
#      a pydantic exception is caught-and-reified").
#    - `ingestion/core/model.py::_check_iana_timezone` (§6.1: the
#      validator-support helper `@field_validator("timezone")` methods
#      delegate to; `zoneinfo.ZoneInfo` signals an unknown IANA name by
#      raising `ZoneInfoNotFoundError`, so the helper must itself
#      catch-and-reraise as `ValueError`).
_TRY_RAISE_ALLOWLIST: frozenset[tuple[str, str]] = frozenset(
    {
        ("ingestion/core/completeness.py", "parse_manifest"),
        ("ingestion/core/model.py", "_check_iana_timezone"),
    }
)

# The field/model validator decorator names exempted by mechanism 1 above.
_VALIDATOR_DECORATOR_NAMES: frozenset[str] = frozenset({"field_validator", "model_validator"})

# Third instance of the same LLD-vs-linter shape (bd conveyer-4ot.26, fixing
# a gap surfaced by conveyer-4ot.14 / m2-effects-stack): §7.3 ("the only
# exception type in the codebase is TransientError") and §7.0 rule 4 require
# `class TransientError(Exception)` in `effects/records.py`, but `Exception`
# is not one of IDIOM's three accepted class shapes (frozen dataclass /
# BaseModel / Enum family). Resolved the same way as the try/raise allowlist
# above: one more narrow, closed-enumeration `(file, class name)` pair — NOT
# a general "any Exception subclass is allowed" exemption. Exactly one entry;
# do not generalize this into a broader rule.
_CLASS_SHAPE_ALLOWLIST: frozenset[tuple[str, str]] = frozenset(
    {
        ("ingestion/effects/records.py", "TransientError"),
    }
)


# --- values ------------------------------------------------------------------


@dataclass(frozen=True)
class Violation:
    rel_path: str
    line: int
    rule: str


# --- shared AST helpers --------------------------------------------------


def _root_matches(dotted: str, banned_root: str) -> bool:
    return dotted == banned_root or dotted.startswith(banned_root + ".")


def _terminal_name(node: ast.expr) -> str | None:
    """Best-effort last identifier of a dotted access: `a.b.c` -> "c"."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _is_purity_scope(rel_path: str) -> bool:
    parts = rel_path.split("/")
    if parts[:1] == ["sources"]:
        return True
    if parts[:2] == ["ingestion", "core"]:
        return True
    return False


# --- checker: imports (purity roots + idiom roots/dotted, both groups) ---


def _check_dotted_import(
    dotted: str, lineno: int, rel_path: str, purity_scope: bool
) -> list[Violation]:
    out: list[Violation] = []
    if purity_scope:
        for banned in _PURITY_BANNED_IMPORT_ROOTS:
            if _root_matches(dotted, banned):
                out.append(Violation(rel_path, lineno, f"purity-banned-import:{banned}"))
                break
    for banned in _IDIOM_BANNED_IMPORT_ROOTS + _IDIOM_BANNED_IMPORT_DOTTED:
        if _root_matches(dotted, banned):
            out.append(Violation(rel_path, lineno, f"idiom-banned-import:{banned}"))
            break
    return out


def _import_violations(tree: ast.Module, rel_path: str, purity_scope: bool) -> list[Violation]:
    out: list[Violation] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                out.extend(_check_dotted_import(alias.name, node.lineno, rel_path, purity_scope))
        elif isinstance(node, ast.ImportFrom):
            module = node.module
            if module is not None:
                out.extend(_check_dotted_import(module, node.lineno, rel_path, purity_scope))
            # `from unittest import mock` — module is "unittest" alone (not
            # banned), the submodule import is named in `names` instead.
            if module == "unittest":
                for alias in node.names:
                    if alias.name == "mock":
                        out.append(
                            Violation(rel_path, node.lineno, "idiom-banned-import:unittest.mock")
                        )
    return out


# --- checker: class shape (idiom, all walked files) -----------------------


def _is_frozen_dataclass_decorator(dec: ast.expr) -> bool:
    if not isinstance(dec, ast.Call):
        return False
    if _terminal_name(dec.func) != "dataclass":
        return False
    return any(
        isinstance(kw, ast.keyword)
        and kw.arg == "frozen"
        and isinstance(kw.value, ast.Constant)
        and kw.value.value is True
        for kw in dec.keywords
    )


def _is_allowed_class(node: ast.ClassDef, rel_path: str) -> bool:
    if (rel_path, node.name) in _CLASS_SHAPE_ALLOWLIST:
        return True
    if any(_is_frozen_dataclass_decorator(dec) for dec in node.decorator_list):
        return True
    base_names = {_terminal_name(base) for base in node.bases}
    if "BaseModel" in base_names:
        return True
    if base_names & _ALLOWED_ENUM_BASE_NAMES:
        return True
    return False


def _class_shape_violations(tree: ast.Module, rel_path: str, purity_scope: bool) -> list[Violation]:
    del purity_scope  # idiom rule applies regardless of scope
    return [
        Violation(rel_path, node.lineno, "idiom-class")
        for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and not _is_allowed_class(node, rel_path)
    ]


# --- checker: banned calls (purity, core/sources only) ---------------------


def _call_violation_name(node: ast.Call) -> str | None:
    func = node.func
    if isinstance(func, ast.Name) and func.id in _BANNED_BARE_CALLS:
        return func.id
    if isinstance(func, ast.Attribute):
        root = _terminal_name(func.value)
        if root is not None and (root, func.attr) in _BANNED_ATTR_CALLS:
            return f"{root}.{func.attr}"
    return None


def _banned_call_violations(tree: ast.Module, rel_path: str, purity_scope: bool) -> list[Violation]:
    if not purity_scope:
        return []
    out: list[Violation] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = _call_violation_name(node)
            if name is not None:
                out.append(Violation(rel_path, node.lineno, f"purity-banned-call:{name}"))
    return out


# --- checker: raise/try (purity, core/sources only, exemptions above) ------


def _allowlisted_control_flow_ids(tree: ast.Module, rel_path: str) -> frozenset[int]:
    """ids of every `Try`/`Raise` node lexically inside one of the two
    hardcoded (file, function) allowlist entries — exempt from both
    `purity-try` and `purity-raise` regardless of shape."""
    ids: set[int] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and (rel_path, node.name) in _TRY_RAISE_ALLOWLIST
        ):
            ids.update(
                id(inner) for inner in ast.walk(node) if isinstance(inner, (ast.Try, ast.Raise))
            )
    return frozenset(ids)


def _is_validator_decorator(dec: ast.expr) -> bool:
    """Matches `field_validator`/`model_validator` in bare (`@field_validator`),
    attribute (`@pydantic.field_validator`), or call
    (`@field_validator("x")`/`@pydantic.field_validator("x")`) form."""
    target = dec.func if isinstance(dec, ast.Call) else dec
    return _terminal_name(target) in _VALIDATOR_DECORATOR_NAMES


def _validator_decorated_raise_ids(tree: ast.Module) -> frozenset[int]:
    """ids of every `Raise` node lexically inside a function whose decorator
    list includes a validator decorator (any form, `@classmethod`-paired or
    not) — exempt from `purity-raise` only; `try` is untouched by this rule."""
    ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and any(
            _is_validator_decorator(dec) for dec in node.decorator_list
        ):
            ids.update(id(inner) for inner in ast.walk(node) if isinstance(inner, ast.Raise))
    return frozenset(ids)


def _control_flow_violations(
    tree: ast.Module, rel_path: str, purity_scope: bool
) -> list[Violation]:
    if not purity_scope:
        return []
    allowlisted = _allowlisted_control_flow_ids(tree, rel_path)
    validator_raises = _validator_decorated_raise_ids(tree)
    out: list[Violation] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Try):
            if id(node) not in allowlisted:
                out.append(Violation(rel_path, node.lineno, "purity-try"))
        elif isinstance(node, ast.Raise):
            if id(node) not in allowlisted and id(node) not in validator_raises:
                out.append(Violation(rel_path, node.lineno, "purity-raise"))
    return out


_CHECKERS: tuple[Callable[[ast.Module, str, bool], list[Violation]], ...] = (
    _import_violations,
    _class_shape_violations,
    _banned_call_violations,
    _control_flow_violations,
)


def lint_source(source: str, rel_path: str) -> tuple[Violation, ...]:
    """Lint one file's already-read source text.

    `rel_path` is a POSIX-style path relative to the `conveyer-ingestion`
    package root (e.g. `"ingestion/core/completeness.py"`); it drives both
    the reported location and which rule group(s) apply.
    """
    tree = ast.parse(source, filename=rel_path)
    purity_scope = _is_purity_scope(rel_path)
    violations = [v for checker in _CHECKERS for v in checker(tree, rel_path, purity_scope)]
    return tuple(sorted(violations, key=lambda v: (v.rel_path, v.line, v.rule)))


# --- filesystem edge (walk + CLI) ------------------------------------------


def _discover_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for base in ("ingestion", "sources"):
        base_dir = root / base
        if base_dir.is_dir():
            files.extend(sorted(base_dir.rglob("*.py")))
    return files


def lint_file(path: Path, root: Path) -> tuple[Violation, ...]:
    rel_path = path.relative_to(root).as_posix()
    source = path.read_text(encoding="utf-8")
    return lint_source(source, rel_path)


def _format_violation(v: Violation) -> str:
    return f"{v.rel_path}:{v.line}:{v.rule}"


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    violations = sorted(
        (v for f in _discover_files(root) for v in lint_file(f, root)),
        key=lambda v: (v.rel_path, v.line, v.rule),
    )
    for v in violations:
        print(_format_violation(v))
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main())
