"""Purity + idiom linter engine — config-as-data (LLD 004.1 I-15, §12.3).

Promoted from `ingestion/tools/purity_linter.py` (LLD 002.1 §12.2): the engine
itself now carries no package-specific knowledge — every rule table, walked
root, and hardcoded `(rel_path, name)` exemption lives in a `LinterConfig`
value, supplied by a config module under `tools/linter_configs/` (one config
module per package: `ingestion.py`, `spine.py`, ...). "One implementation, N
configurations" (004 D-8).

A stdlib `ast` walk over a config's `walk_roots` (relative to the repo root,
this module's parent directory). Two rule groups, same as before the
promotion:

* PURITY — applies within a `LinterConfig`'s `profiles`: a `ScopeProfile`
  matches a subset of `walk_roots` by path prefix (e.g. ingestion's single
  "purity" profile matches `sources/**` and `ingestion/core/**`; spine's
  `core`/`frames-transforms`/`effects-stages` split matches three disjoint
  subtrees, LLD §12.3) and carries its own banned imports/calls and an
  optional `try`/`raise` ban.
* IDIOM — applies to every file under `walk_roots` regardless of profile:
  `class` statements banned unless `@dataclass(frozen=True)`, a pydantic
  `BaseModel` subclass, or an `enum.Enum` subclass, except for a config's
  `class_shape_allowlist` of hardcoded `(rel_path, class name)` pairs. Also
  banned everywhere: a config's `idiom_banned_import_roots`/`_dotted`.

Two further profile-scoped rule shapes, generalizing the same best-effort AST
mechanism: `banned_attr_names` bans an attribute access by name alone,
regardless of receiver AND regardless of position in a chain (e.g. spine's
`.collect()`/`.read()` Spark-API ban, where the receiver isn't a fixed name)
— every `ast.Attribute` node in the file is checked, not only one that is
itself the outermost `Call.func` of a call expression, so a bare mid-chain
lookup like `df.sparkSession.read.parquet(path)` trips on `sparkSession` and
`read` even though neither is ever directly called (fixed post-M5, critique
F3: the prior outermost-call-only check was the "just one lookup" erosion
vector — see `_attr_name_violations`), and may be licensed by name in exactly
one file via the config-level `banned_attr_exemption` — a `(rel_path,
attr_name)` pair set (007.1 [DC2-2], B11-local: this mechanism did not exist
before that bead; `tools/linter_configs/spine.py` used to carry an explicit
comment recording the gap); `string_sql_sinks` + the config-level
`string_sql_exemption` implement the string-SQL review rule (an f-string/
`.format()`/`%`-formatted argument flowing into a named sink call), exempted
only by the same `(rel_path, function)` allowlist mechanism as the try/raise
and class-shape exemptions.

This file and its own tests live under `tools/` and `tests/`, which are *not*
part of any config's `walk_roots` — the linter is exempt from its own rules
by construction — but it is still written in the same functional style:
plain values (`Violation`, `LinterConfig`, `ScopeProfile`) and plain
functions, no class beyond those three frozen dataclasses.
"""

from __future__ import annotations

import ast
import importlib
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

# --- config-as-data ---------------------------------------------------------


@dataclass(frozen=True)
class ScopeProfile:
    """One scoped rule bundle: which paths it applies to, and what it bans.

    A `LinterConfig` may declare several profiles (e.g. spine's core /
    frames-transforms / effects-stages split, §12.3); ingestion declares one
    ("purity") reproducing its prior single purity/idiom split verbatim.
    """

    name: str
    path_prefixes: tuple[tuple[str, ...], ...]
    banned_import_roots: tuple[str, ...] = ()
    banned_bare_calls: frozenset[str] = frozenset()
    banned_attr_calls: frozenset[tuple[str, str]] = frozenset()
    banned_attr_names: frozenset[str] = frozenset()
    ban_try_raise: bool = False
    string_sql_sinks: frozenset[str] = frozenset()


@dataclass(frozen=True)
class LinterConfig:
    """Everything the engine needs to lint one package: walk roots + rule data.

    `walk_roots` and every `ScopeProfile.path_prefixes` entry are relative to
    `package_root` (itself relative to the repo root — this module's own
    parent directory), NOT to the repo root directly: each uv-workspace
    module (`ingestion/`, `spine/`, ...) nests its actual package one level
    down (`ingestion/ingestion/`, `spine/spine/`), alongside sibling
    `sources/`/`tests/`/`tools/` dirs, so `package_root` names that module
    directory (e.g. `"ingestion"`) and `walk_roots` name its children (e.g.
    `("ingestion", "sources")`) exactly as they did before the engine's
    promotion out of that module — only the engine's own address moved.

    Idiom rules (banned imports, class-shape) are engine-wide (apply to every
    walked file) rather than profile-scoped, matching the pre-promotion
    "idiom applies everywhere, purity applies to a subset" split.

    `banned_attr_exemption` (007.1 [DC2-2], bead `conveyer-6pg.23`, B11-local)
    is the per-file exemption mechanism `banned_attr_names` lacked until this
    revision — `tools/linter_configs/spine.py` used to document the gap
    explicitly (`_SPARK_BANNED_ATTR_NAMES`'s own comment: "`banned_attr_names`
    has NO `(file, function)` exemption mechanism yet ... a future bead
    needing a real `F.count` aggregate ... must add that exemption mechanism
    to `tools/purity_linter.py` first, not silently work around this ban").
    Same shape and granularity as `class_shape_allowlist`/`string_sql_
    exemption` — a hardcoded `(rel_path, attr_name)` pair set — but scoped to
    the WHOLE FILE, not a named function: `banned_attr_names`' own rule fires
    on every `ast.Attribute` node regardless of which function (or none) it
    sits in, so a function-scoped allowlist (walking one `FunctionDef`'s
    subtree, `_allowlisted_control_flow_ids`'s own technique) cannot express
    "this ONE blessed module may render the ban's construction, anywhere in
    the file" — the licensed shape §15's own row names ("the spine linter's
    banned-attribute profile bans `.overwrite(` outside the one blessed
    rebuild/swap module (per-file exemption)"). `[DS2-2]`'s own governance
    note (a config-module comment, not an engine concern): growth of a
    profile's `banned_attr_names` OR of this exemption set carries the same
    platform-data-architecture-owner-plus-security-gate countersign as any
    other licensed hole in the construction.
    """

    name: str
    walk_roots: tuple[str, ...]
    package_root: str = ""
    profiles: tuple[ScopeProfile, ...] = ()
    idiom_banned_import_roots: tuple[str, ...] = ()
    idiom_banned_import_dotted: tuple[str, ...] = ()
    allowed_enum_base_names: frozenset[str] = field(
        default_factory=lambda: frozenset(
            {"Enum", "IntEnum", "StrEnum", "Flag", "IntFlag"}
        )
    )
    try_raise_allowlist: frozenset[tuple[str, str]] = frozenset()
    validator_decorator_names: frozenset[str] = frozenset()
    class_shape_allowlist: frozenset[tuple[str, str]] = frozenset()
    string_sql_exemption: frozenset[tuple[str, str]] = frozenset()
    banned_attr_exemption: frozenset[tuple[str, str]] = frozenset()


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


def _matching_profiles(rel_path: str, config: LinterConfig) -> tuple[ScopeProfile, ...]:
    parts = tuple(rel_path.split("/"))
    return tuple(
        profile
        for profile in config.profiles
        if any(parts[: len(prefix)] == prefix for prefix in profile.path_prefixes)
    )


# --- checker: imports (idiom roots everywhere + profile-banned roots) ------


def _check_dotted_import(
    dotted: str,
    lineno: int,
    rel_path: str,
    config: LinterConfig,
    profiles: tuple[ScopeProfile, ...],
) -> list[Violation]:
    out: list[Violation] = []
    for profile in profiles:
        for banned in profile.banned_import_roots:
            if _root_matches(dotted, banned):
                out.append(
                    Violation(rel_path, lineno, f"purity-banned-import:{banned}")
                )
                break
    for banned in config.idiom_banned_import_roots + config.idiom_banned_import_dotted:
        if _root_matches(dotted, banned):
            out.append(Violation(rel_path, lineno, f"idiom-banned-import:{banned}"))
            break
    return out


def _import_violations(
    tree: ast.Module,
    rel_path: str,
    config: LinterConfig,
    profiles: tuple[ScopeProfile, ...],
) -> list[Violation]:
    out: list[Violation] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                out.extend(
                    _check_dotted_import(
                        alias.name, node.lineno, rel_path, config, profiles
                    )
                )
        elif isinstance(node, ast.ImportFrom):
            module = node.module
            if module is not None:
                out.extend(
                    _check_dotted_import(
                        module, node.lineno, rel_path, config, profiles
                    )
                )
            # `from unittest import mock` — module is "unittest" alone (not
            # banned), the submodule import is named in `names` instead.
            if module == "unittest":
                for alias in node.names:
                    if alias.name == "mock":
                        out.append(
                            Violation(
                                rel_path,
                                node.lineno,
                                "idiom-banned-import:unittest.mock",
                            )
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


def _is_allowed_class(node: ast.ClassDef, rel_path: str, config: LinterConfig) -> bool:
    if (rel_path, node.name) in config.class_shape_allowlist:
        return True
    if any(_is_frozen_dataclass_decorator(dec) for dec in node.decorator_list):
        return True
    base_names = {_terminal_name(base) for base in node.bases}
    if "BaseModel" in base_names:
        return True
    if base_names & config.allowed_enum_base_names:
        return True
    return False


def _class_shape_violations(
    tree: ast.Module,
    rel_path: str,
    config: LinterConfig,
    profiles: tuple[ScopeProfile, ...],
) -> list[Violation]:
    del profiles  # idiom rule applies regardless of profile
    return [
        Violation(rel_path, node.lineno, "idiom-class")
        for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef)
        and not _is_allowed_class(node, rel_path, config)
    ]


# --- checker: banned calls (profile-scoped) ---------------------------------


def _call_violation_name(node: ast.Call, profile: ScopeProfile) -> str | None:
    func = node.func
    if isinstance(func, ast.Name) and func.id in profile.banned_bare_calls:
        return func.id
    if isinstance(func, ast.Attribute):
        root = _terminal_name(func.value)
        if root is not None and (root, func.attr) in profile.banned_attr_calls:
            return f"{root}.{func.attr}"
    return None


def _banned_call_violations(
    tree: ast.Module,
    rel_path: str,
    config: LinterConfig,
    profiles: tuple[ScopeProfile, ...],
) -> list[Violation]:
    del config
    out: list[Violation] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for profile in profiles:
                name = _call_violation_name(node, profile)
                if name is not None:
                    out.append(
                        Violation(rel_path, node.lineno, f"purity-banned-call:{name}")
                    )
    return out


# --- checker: banned attribute names (profile-scoped, ANY position) --------


def _attr_name_violations(
    tree: ast.Module,
    rel_path: str,
    config: LinterConfig,
    profiles: tuple[ScopeProfile, ...],
) -> list[Violation]:
    """Flags every `ast.Attribute` node whose `.attr` is in a matching
    profile's `banned_attr_names` — bare mid-chain access (`df.sparkSession`)
    and call-position access (`source.read()`) alike. Deliberately NOT
    restricted to `ast.Call.func` position (see module docstring, critique
    F3): `df.sparkSession.read.parquet(path)` must trip on `sparkSession`
    and `read` even though neither is itself called directly — only the
    innermost `parquet` sits in call position, and `parquet` isn't banned.

    A plain `ast.Name` (e.g. a local variable literally named `read`) is
    never an `ast.Attribute` and so can never trip this rule — only actual
    attribute access on some receiver does.

    `config.banned_attr_exemption` (007.1 [DC2-2], B11-local) is the
    per-file exemption this rule lacked at first authoring — a `(rel_path,
    attr_name)` pair in that set silences ONLY that one name in that one
    file, leaving every other banned name (and every other file) fully
    enforced; see `LinterConfig`'s own docstring for the full account."""
    out: list[Violation] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            for profile in profiles:
                if (
                    node.attr in profile.banned_attr_names
                    and (
                        rel_path,
                        node.attr,
                    )
                    not in config.banned_attr_exemption
                ):
                    out.append(
                        Violation(
                            rel_path, node.lineno, f"purity-banned-attr:{node.attr}"
                        )
                    )
    return out


# --- checker: raise/try (profile-scoped, engine-wide allowlists) -----------


def _allowlisted_control_flow_ids(
    tree: ast.Module, rel_path: str, config: LinterConfig
) -> frozenset[int]:
    """ids of every `Try`/`Raise` node lexically inside one of a config's
    hardcoded `(file, function)` allowlist entries — exempt from both
    `purity-try` and `purity-raise` regardless of shape."""
    ids: set[int] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and (rel_path, node.name) in config.try_raise_allowlist
        ):
            ids.update(
                id(inner)
                for inner in ast.walk(node)
                if isinstance(inner, (ast.Try, ast.Raise))
            )
    return frozenset(ids)


def _is_validator_decorator(dec: ast.expr, config: LinterConfig) -> bool:
    """Matches a config's validator-decorator names in bare, attribute, or
    call form."""
    target = dec.func if isinstance(dec, ast.Call) else dec
    return _terminal_name(target) in config.validator_decorator_names


def _validator_decorated_raise_ids(
    tree: ast.Module, config: LinterConfig
) -> frozenset[int]:
    """ids of every `Raise` node lexically inside a function whose decorator
    list includes a validator decorator — exempt from `purity-raise` only;
    `try` is untouched by this rule."""
    ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and any(
            _is_validator_decorator(dec, config) for dec in node.decorator_list
        ):
            ids.update(
                id(inner) for inner in ast.walk(node) if isinstance(inner, ast.Raise)
            )
    return frozenset(ids)


def _control_flow_violations(
    tree: ast.Module,
    rel_path: str,
    config: LinterConfig,
    profiles: tuple[ScopeProfile, ...],
) -> list[Violation]:
    if not any(profile.ban_try_raise for profile in profiles):
        return []
    allowlisted = _allowlisted_control_flow_ids(tree, rel_path, config)
    validator_raises = _validator_decorated_raise_ids(tree, config)
    out: list[Violation] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Try):
            if id(node) not in allowlisted:
                out.append(Violation(rel_path, node.lineno, "purity-try"))
        elif isinstance(node, ast.Raise):
            if id(node) not in allowlisted and id(node) not in validator_raises:
                out.append(Violation(rel_path, node.lineno, "purity-raise"))
    return out


# --- checker: string-SQL (profile-scoped sinks, engine-wide exemption) ----


def _string_sql_exempt_ids(
    tree: ast.Module, rel_path: str, config: LinterConfig
) -> frozenset[int]:
    """ids of every `Call` node lexically inside one of the `(file, function)`
    entries in `config.string_sql_exemption` — the single hardcoded escape
    hatch for a rendered MERGE (§12.3), same allowlist-by-function mechanism
    as `_allowlisted_control_flow_ids`."""
    ids: set[int] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and (rel_path, node.name) in config.string_sql_exemption
        ):
            ids.update(
                id(inner) for inner in ast.walk(node) if isinstance(inner, ast.Call)
            )
    return frozenset(ids)


def _is_stringy_sql_arg(node: ast.expr) -> bool:
    """Best-effort: an f-string, a `.format(...)` call, or a `%`-formatted
    string flowing into a sink argument."""
    if isinstance(node, ast.JoinedStr):
        return True
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "format"
    ):
        return True
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod):
        return True
    return False


def _string_sql_violations(
    tree: ast.Module,
    rel_path: str,
    config: LinterConfig,
    profiles: tuple[ScopeProfile, ...],
) -> list[Violation]:
    sinks: frozenset[str] = frozenset()
    for profile in profiles:
        sinks |= profile.string_sql_sinks
    if not sinks:
        return []
    exempt = _string_sql_exempt_ids(tree, rel_path, config)
    out: list[Violation] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in sinks
            and id(node) not in exempt
            and any(_is_stringy_sql_arg(arg) for arg in node.args)
        ):
            out.append(
                Violation(rel_path, node.lineno, f"idiom-string-sql:{node.func.attr}")
            )
    return out


_CHECKERS: tuple[
    Callable[
        [ast.Module, str, LinterConfig, tuple[ScopeProfile, ...]], list[Violation]
    ],
    ...,
] = (
    _import_violations,
    _class_shape_violations,
    _banned_call_violations,
    _attr_name_violations,
    _control_flow_violations,
    _string_sql_violations,
)


def lint_source(
    source: str, rel_path: str, config: LinterConfig
) -> tuple[Violation, ...]:
    """Lint one file's already-read source text.

    `rel_path` is a POSIX-style path relative to the repo root (e.g.
    `"ingestion/core/completeness.py"`); it drives both the reported location
    and which of `config`'s profiles apply.
    """
    tree = ast.parse(source, filename=rel_path)
    profiles = _matching_profiles(rel_path, config)
    violations = [
        v for checker in _CHECKERS for v in checker(tree, rel_path, config, profiles)
    ]
    return tuple(sorted(violations, key=lambda v: (v.rel_path, v.line, v.rule)))


# --- filesystem edge (walk + CLI) ------------------------------------------


def _discover_files(root: Path, config: LinterConfig) -> list[Path]:
    """Walk `config.walk_roots` under `root`. Missing roots are skipped
    gracefully (e.g. spine's `walk_roots` name paths that don't exist yet)."""
    files: list[Path] = []
    for base in config.walk_roots:
        base_dir = root / base
        if base_dir.is_dir():
            files.extend(sorted(base_dir.rglob("*.py")))
    return files


def lint_file(path: Path, root: Path, config: LinterConfig) -> tuple[Violation, ...]:
    rel_path = path.relative_to(root).as_posix()
    source = path.read_text(encoding="utf-8")
    return lint_source(source, rel_path, config)


def _format_violation(v: Violation) -> str:
    return f"{v.rel_path}:{v.line}:{v.rule}"


def _load_config(config_name: str) -> LinterConfig:
    """Load `tools/linter_configs/<config_name>.py`'s `CONFIG` by name. Relies
    on `tools/` already being on `sys.path` — true both for the CLI (Python
    inserts a script's own directory at `sys.path[0]`) and for callers that
    set up `sys.path` themselves (e.g. the test suite)."""
    module = importlib.import_module(f"linter_configs.{config_name}")
    config: LinterConfig = module.CONFIG
    return config


def main(
    argv: Sequence[str] | None = None,
    *,
    config: LinterConfig | None = None,
    root: Path | None = None,
) -> int:
    """CLI entrypoint: `python tools/purity_linter.py <config-name>`.

    `config` may be passed directly (bypassing config-name resolution) —
    used by callers, including tests, that already hold a `LinterConfig`.
    `root` may likewise be passed directly (bypassing `package_root`-relative
    discovery) — used by tests that supply their own synthetic tree.
    """
    if config is None:
        args = list(sys.argv[1:] if argv is None else argv)
        if not args:
            print("usage: purity_linter.py <config-name>", file=sys.stderr)
            return 2
        config = _load_config(args[0])
    if root is None:
        repo_root = Path(__file__).resolve().parent.parent
        root = repo_root / config.package_root if config.package_root else repo_root
    violations = sorted(
        (v for f in _discover_files(root, config) for v in lint_file(f, root, config)),
        key=lambda v: (v.rel_path, v.line, v.rule),
    )
    for v in violations:
        print(_format_violation(v))
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main())
