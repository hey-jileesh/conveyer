"""Ingestion's linter config (LLD 004.1 §12.3, I-15) — VERBATIM transfer of
the rule tables that lived as module-level constants in the pre-promotion
`ingestion/tools/purity_linter.py` (LLD 002.1 §12.2). Nothing here changes
the rules themselves, only where they live.

* PURITY — a single `"purity"` profile matching `sources/**` and
  `ingestion/core/**` (effects and entrypoints are exempt): banned imports of
  anything that could perform I/O or introduce nondeterminism, banned calls
  to the same effect, and a ban on `raise`/`try` (defects are values, per
  002.1 §7.0 rule 4) — with two narrow, hardcoded exemptions resolving the
  §6.1/§12.2 contradiction (a pydantic validator MUST raise `ValueError` to
  signal failure; see `conveyer-4ot.24`): `raise` inside a
  `@field_validator`/`@model_validator` method is exempt from
  `purity-raise`, and exactly two hardcoded `(file, function)` pairs are
  exempt from both `purity-try` and `purity-raise` entirely.
* IDIOM — applies to ALL of `ingestion/**` and `sources/**`: `class`
  statements are banned unless they are `@dataclass(frozen=True)`, a
  pydantic `BaseModel` subclass, or an `enum.Enum` subclass, EXCEPT for one
  narrow, hardcoded `(file, class name)` exemption resolving the
  §7.3/§12.2 contradiction (`class TransientError(Exception)` is "the only
  exception type in the codebase," §7.3; see `conveyer-4ot.26`) —
  `ingestion/effects/records.py::TransientError` only, not a general
  Exception-subclass exemption. Also banned everywhere: a small list of
  FP-framework/mocking imports.
"""

from __future__ import annotations

import purity_linter

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

# Director adjudication of the LLD §6.1-vs-§12.2 contradiction (conveyer-4ot.24):
# §6.1 mandates `@field_validator`/`@model_validator` methods that
# `raise ValueError(...)` — pydantic v2 has no non-raising failure protocol
# for custom validators — while §12.2/§7.0 rule 4 bans `raise`/`try` outright
# in `core/`. Resolved by two independent, narrow mechanisms:
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

_PURITY_PROFILE = purity_linter.ScopeProfile(
    name="purity",
    path_prefixes=(("sources",), ("ingestion", "core")),
    banned_import_roots=_PURITY_BANNED_IMPORT_ROOTS,
    banned_bare_calls=_BANNED_BARE_CALLS,
    banned_attr_calls=_BANNED_ATTR_CALLS,
    ban_try_raise=True,
)

CONFIG = purity_linter.LinterConfig(
    name="ingestion",
    walk_roots=("ingestion", "sources"),
    # the `ingestion` uv-workspace module directory (`conveyer/ingestion/`),
    # NOT the repo root — see LinterConfig's docstring.
    package_root="ingestion",
    profiles=(_PURITY_PROFILE,),
    idiom_banned_import_roots=_IDIOM_BANNED_IMPORT_ROOTS,
    idiom_banned_import_dotted=_IDIOM_BANNED_IMPORT_DOTTED,
    try_raise_allowlist=_TRY_RAISE_ALLOWLIST,
    validator_decorator_names=_VALIDATOR_DECORATOR_NAMES,
    class_shape_allowlist=_CLASS_SHAPE_ALLOWLIST,
)
