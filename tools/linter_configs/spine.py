"""Spine's linter config (LLD 004.1 §12.3, I-15) — four profiles.

Finalized at M5 (bead `conveyer-nvh.28`, R-11): the fixture corpus in
`spine/tests/unit/linter_fixtures/` + `spine/tests/unit/
test_linter_spine_corpus.py` exercises every bullet below against each
profile (the five §12.3-named cases: `fail_transforms_spark_read.py`,
`fail_transforms_collect.py`, `fail_core_pyspark_import.py`,
`fail_effects_fstring_where.py`, `pass_frames_functions_only.py`), and a
real-tree regression test confirms the actual `spine/spine/**` +
`spine/pipelines/**` source tree reports zero violations under this exact
config (`make -C spine lint` is R-11's evidence at the repo-lint level; the
unit corpus is the fast, fixture-driven layer underneath it). The
`string_sql_exemption` naming `effects/spark.py::render_merge` is verified
against the real function (not a stale name after a rename) by that same
corpus test — `render_merge`'s own docstring records why the exemption is
currently a documented no-op in practice (`spark.sql(sql_text)` is called
with a plain variable, never an inline f-string), kept as defense in depth
rather than removed.

Extended post-M5 (design-critique finding F3, bead `conveyer-nvh.42`) with a
fourth, package-wide profile (below) plus one more entry in the Spark
attribute-name ban list (`sparkSession`) — see `tools/purity_linter.py`'s
own module docstring for the matching engine-side fix: `banned_attr_names`
now flags ANY `ast.Attribute` node by name, not only one sitting in the
outermost `Call.func` position, closing the "just one lookup" gap where
`df.sparkSession.read.parquet(path)` passed every profile untouched
(`sparkSession`/`read` were never themselves called; only the innermost,
unbanned `parquet` was).

`walk_roots` below are relative to `package_root="spine"` (the uv-workspace
module dir, NOT the repo root) — `_discover_files` tolerates a missing root
gracefully, which is how this config loaded cleanly pre-M1 before `spine/`
existed; that tolerance is now exercised only by a synthetic tree in tests,
not by the real walk.

* `spine/core/**`: ingestion-core purity rules **plus** a banned import of
  `pyspark` (`awsglue` moved to the package-wide profile below, F3(b)).
* `spine/frames/**` and pipeline `transforms.py` (walked in pipeline-package
  CI, and over `tests/exemplar/**` in spine CI): banned imports `boto3`,
  `botocore`, `requests`, `urllib`, `http`, `socket`, `subprocess`, `os`,
  `sys`, `pathlib`, `spine.context`, `spine.effects`, `spine.config`
  (`awsglue` moved to the package-wide profile below, F3(b); D-9 —
  transforms import *nothing* of the spine; `frames/` additionally may not
  import `spine.effects` or `spine.context` [C-5]); banned attribute
  accesses by name alone, regardless of receiver AND regardless of position
  in an attribute chain (best-effort AST, same mechanism as ingestion's,
  generalized — `banned_attr_names` vs. ingestion's `(root, attr)` pairs):
  `SparkSession`, `getOrCreate`, `newSession`, `sparkContext`,
  `sparkSession`, `read`, `write`, `writeTo`, `sql`, `collect`, `toPandas`,
  `toLocalIterator`, `foreach`, `foreachPartition`, `checkpoint`, `cache`,
  `persist`, `take`, `head`, `show`, `count` (critique F1, bead
  conveyer-azr.30 — closes the one hole `.count()` sat in, previously the
  sole eager Spark action absent from this list; see `_SPARK_BANNED_ATTR_
  NAMES`'s own comment for the `F.count` aggregate trade-off this closes
  without yet needing); banned calls
  `open`/`eval`/`exec`/`__import__`, `datetime.now`-family,
  `uuid.uuid4`-family.
* `spine/effects/**` + `spine/stages/**`: idiom rules (engine-wide, below)
  plus a string-SQL review rule [S-6]: f-strings/`format`/`%` feeding
  `where`, `filter`, `selectExpr`, or `spark.sql` are banned — the rendered
  MERGE in `effects/spark.py` is the single hardcoded exemption (by `(file,
  function)`, the ingestion exemption mechanism).
* Package-wide (`path_prefixes=(("spine",), ("pipelines",))`, matching
  every file under both of `CONFIG.walk_roots`, F3(b)): a single banned
  import of `awsglue` (I-14, "no awsglue anywhere in spine/") — this is the
  ONLY place that ban now lives (previously duplicated across the `core`
  and `frames-transforms` profiles above, and entirely absent from
  `effects/`, `stages/`, `entrypoints/`, `bootstrap/`, and spine's
  top-level modules — `run.py`, `binding.py`, `config.py`, `context.py`,
  `observability.py` — which match none of the other three profiles' path
  prefixes).
* Idiom rules (frozen dataclass / BaseModel / Enum only; no FP frameworks;
  no `unittest.mock`) apply to **all** of `spine/**` (engine-wide, not
  profile-scoped — same as ingestion), `TransientError` exempted by config
  as in ingestion.
"""

from __future__ import annotations

import purity_linter

# Same mechanism as ingestion's two-entry allowlist (LLD §6 preamble docstring
# in ingestion/core/model.py): plain validator-SUPPORT helpers -- not
# themselves `@field_validator`/`@model_validator`-decorated, so
# `_validator_decorated_raise_ids` doesn't reach their `raise` -- that are
# called FROM a decorated method. `spine/core/model.py`'s
# `_check_pipeline_slug_grammar` and `check_qualified_table` are exactly this
# shape: shared raise-only logic (no `try`) reused across several fields
# (`pipeline` on two models; the four table fields plus `CoEffectDecl.table`),
# factored out rather than duplicated inline in every validator body.
#
# M1 (bead conveyer-nvh.14) additions -- `core/naming.py` and `core/merge.py`
# are boundary-validation helpers of the SAME shape (raise-only, no `try`),
# called from composition functions rather than from a pydantic validator:
# `naming._check_pipeline_slug_grammar` (own copy of the same grammar,
# `core/model.py`'s version being private to that file), `naming.
# execution_name`/`rerun_execution_name` (UUIDv5/rerun-number checks before
# composing an execution name, I-22), `naming.check_object_uris` (I-22's
# self-consistency check, "failure is a binding defect"), and `merge.
# _check_identifier` (§6.7/[S-10]'s identifier grammar, checked before any
# SQL is assembled). `run_facts._stage_fields`'s `raise` is a different
# shape -- an internal exhaustiveness guard over the fixed stage-name set,
# not input validation -- but is the same raise-only (no `try`) construct,
# so it needs the same allowlist mechanism.
#
# M5 (bead conveyer-nvh.26) addition -- `naming.check_qualified_table` is a
# NEW own-copy of `model.py`'s function of the same name (previously
# imported from `model.py`; re-derived locally instead so `naming.py` stays
# import-free of `model.py`'s pydantic dependency, a §7.1/I-8 zip-purity
# requirement for `entrypoints/router.py`, which imports `naming`). Same
# raise-only shape as its model.py sibling, needing the same allowlist entry.
#
# 005.1 N0 (bead conveyer-azr.10, n0-canonical) addition --
# `canonical.py::_reject` is a single raise-only helper (`raise
# ValueError(f"canonical_json: {message}")`, `-> NoReturn`) that every
# rejection in `core/canonical.py` (float, non-finite Decimal, naive
# datetime [DC-3], non-str object key, unsupported type) calls through,
# rather than each call site spelling its own `raise` -- one allowlist entry
# instead of five, same shape as this list's other raise-only helpers.
#
# 005.1 N0 (bead conveyer-azr.11, n0-models) addition --
# `contract.py::parse_column_type` (§3.2, D-5's "single interpreter of the
# type grammar") is called from `core/model.py`'s `ColumnSpec` model
# validators (decimal bounds, temporal fmt, min/max), not itself a
# `@field_validator`/`@model_validator` -- same raise-only shape as this
# list's other cross-module helpers. `model.py::_check_single_ascii_printable`
# is the same shape again: a plain validator-support helper (§3.1's
# `DialectModel.delimiter`/`.quote`, "exactly one ASCII printable char"),
# shared across two fields' `@field_validator`s rather than duplicated in
# each one -- exactly `_check_pipeline_slug_grammar`'s pattern.
#
# 005.1 N0 (bead conveyer-azr.12, n0-reading) addition --
# `reading.py::parse_line` (§5.3, A-1) needs `try` (not just `raise`) to
# convert a strict `csv.Error` into a `ParsedLine` value -- defects-as-values,
# not an exception escaping the parse. Unlike this list's other entries, this
# is the corpus's first exercise of the `purity-try` side of `ban_try_raise`
# via the allowlist mechanism (every prior entry only needed the `raise` half)
# -- §12.6 item 1 pins it with a dedicated MUST-PASS fixture
# (`pass_core_parse_line_try.py`) in `test_linter_spine_corpus.py`.
#
# critique F3 (bead conveyer-azr.30) addition --
# `reading.py::multiline_records` (§5.5, moved here from `effects/spark.py`)
# needs `try` for the identical reason `parse_line` does, one function down:
# its `while True: try: tokens = next(reader) except StopIteration: return`
# loop turns the generator-exhaustion signal into a plain return, rather
# than letting `StopIteration` escape as a raised exception -- same
# `purity-try` shape as `parse_line`'s own `csv.Error` conversion.
_TRY_RAISE_ALLOWLIST: frozenset[tuple[str, str]] = frozenset(
    {
        ("spine/core/model.py", "_check_pipeline_slug_grammar"),
        ("spine/core/model.py", "check_qualified_table"),
        ("spine/core/model.py", "_check_single_ascii_printable"),
        ("spine/core/naming.py", "_check_pipeline_slug_grammar"),
        ("spine/core/naming.py", "execution_name"),
        ("spine/core/naming.py", "rerun_execution_name"),
        ("spine/core/naming.py", "check_object_uris"),
        ("spine/core/naming.py", "check_qualified_table"),
        ("spine/core/merge.py", "_check_identifier"),
        ("spine/core/run_facts.py", "_stage_fields"),
        ("spine/core/canonical.py", "_reject"),
        ("spine/core/contract.py", "parse_column_type"),
        ("spine/core/reading.py", "parse_line"),
        ("spine/core/reading.py", "multiline_records"),
    }
)

# ingestion-core purity rules, reused verbatim as the base for spine/core, per
# "core = ingestion-core rules + pyspark/awsglue ban" (§12.3).
_INGESTION_CORE_BANNED_IMPORT_ROOTS: tuple[str, ...] = (
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

_BANNED_BARE_CALLS: frozenset[str] = frozenset({"open", "eval", "exec", "__import__"})

_DATETIME_UUID_ATTR_CALLS: frozenset[tuple[str, str]] = frozenset(
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

_CORE_PROFILE = purity_linter.ScopeProfile(
    name="core",
    path_prefixes=(("spine", "core"),),
    # `awsglue` is NOT repeated here — I-14 ("no awsglue anywhere in
    # spine/") is a package-wide rule, carried by `_PACKAGE_WIDE_PROFILE`
    # below (critique F3(b)), not duplicated per profile.
    banned_import_roots=_INGESTION_CORE_BANNED_IMPORT_ROOTS + ("pyspark",),
    banned_bare_calls=_BANNED_BARE_CALLS,
    banned_attr_calls=_DATETIME_UUID_ATTR_CALLS,
    ban_try_raise=True,
)

# "spine/frames/** and pipeline transforms.py" (§12.3) — D-9: transforms
# import nothing of the spine; frames/ additionally may not import
# spine.effects or spine.context [C-5].
_FRAMES_TRANSFORMS_BANNED_IMPORT_ROOTS: tuple[str, ...] = (
    "boto3",
    "botocore",
    "requests",
    "urllib",
    "http",
    "socket",
    "subprocess",
    "os",
    "sys",
    "pathlib",
    # `awsglue` is NOT repeated here — see `_CORE_PROFILE`'s same note;
    # `_PACKAGE_WIDE_PROFILE` below is the single source of that ban.
    "spine.context",
    "spine.effects",
    "spine.config",
)

# critique F1 (bead conveyer-azr.30): `count` closes the one hole this list
# left open (`frames/quarantine.py::_assert_business_reason_grammar`, since
# deleted, routed a `.count()` + raise through it deliberately, precisely
# BECAUSE `take`/`collect` were banned and `count` was not — the canonical
# "guardrails, not steering" erosion the critique names). No `frames/**`/
# pipeline `transforms.py` call site uses the LEGITIMATE `F.count(...)`
# aggregate today (verified: zero real-tree hits), so this ban is safe to
# add outright — but `_attr_name_violations` (`tools/purity_linter.py`)
# flags a bare attribute name regardless of receiver, so `F.count(...)`
# WOULD trip this same rule the day one is genuinely needed. Unlike
# `try_raise_allowlist`/`string_sql_exemption`, `banned_attr_names` has NO
# `(file, function)` exemption mechanism yet (`_attr_name_violations` never
# consults one) — a future bead needing a real `F.count` aggregate under
# `frames/**` must add that exemption mechanism to `tools/purity_linter.py`
# first, not silently work around this ban.
_SPARK_BANNED_ATTR_NAMES: frozenset[str] = frozenset(
    {
        "SparkSession",
        "getOrCreate",
        "newSession",
        "sparkContext",
        "sparkSession",
        "read",
        "write",
        "writeTo",
        "sql",
        "collect",
        "toPandas",
        "toLocalIterator",
        "foreach",
        "foreachPartition",
        "checkpoint",
        "cache",
        "persist",
        "take",
        "head",
        "show",
        "count",
    }
)

# 005.1 N1 (bead conveyer-azr.14, §12.6 item 2): the string-SQL sink names
# reviewed under BOTH `_FRAMES_TRANSFORMS_PROFILE` and `_EFFECTS_STAGES_PROFILE`
# below — one authored set, not two that could drift apart. `expr` is the
# addition this bead makes (was `{"where", "filter", "selectExpr", "sql"}`,
# effects-stages-only): `F.expr(...)` is PySpark 3.5's only way to build a
# `try_cast` expression (no `.try_cast()` Column method until 4.0), so an
# f-string/`.format()`/`%`-formatted argument flowing into `F.expr(...)` is
# now reviewed the same as the other three sinks, in BOTH profiles —
# `frames/checks.py::_typed_expr` (§6.2) is the one `frames/` call site that
# builds such text, and `_STRING_SQL_EXEMPTION` below only names a
# MEANINGFUL escape hatch for it if `_FRAMES_TRANSFORMS_PROFILE`'s own sink
# set would otherwise catch it.
_STRING_SQL_SINKS: frozenset[str] = frozenset(
    {"where", "filter", "selectExpr", "sql", "expr"}
)

_FRAMES_TRANSFORMS_PROFILE = purity_linter.ScopeProfile(
    name="frames-transforms",
    # spine/frames/** in spine CI; pipeline transforms.py walked in
    # pipeline-package CI and over tests/exemplar/** in spine CI (§12.3).
    # `tests/exemplar` (not `spine/tests/exemplar`) because rel_paths are
    # relative to CONFIG.package_root ("spine"), i.e. the `spine` uv-
    # workspace module dir, whose own `tests/` sibling holds the exemplar
    # (LLD §4).
    #
    # `("pipelines",)` added by bead conveyer-nvh.22 (M3, identity exemplar):
    # architect decision D-2 moved pipeline transforms code OUT of
    # `tests/exemplar/<pipeline>/transforms.py` and into a real
    # `pipelines.<pipeline>.transforms` module (`spine/pipelines/<pipeline>/
    # transforms.py`) so I-10's `^pipelines\.` importlib-namespace grammar
    # holds identically in tests and once deployed — the transforms-profile
    # bans (D-9: transforms import *nothing* of the spine) now need to
    # follow that code to its real location. `("tests", "exemplar")`
    # is left in place for the SAME reason D-2 makes it low-risk: post-D-2,
    # nothing under `tests/exemplar/**` is pure transform code anymore (only
    # `pipeline.yaml`, fixture CSVs, and ordinary pytest test files, which
    # legitimately need `pathlib`/`open` this profile bans) — walking it
    # would flag legitimate test I/O, not catch a real transform-purity
    # defect, so `walk_roots` deliberately does NOT include it (see
    # `CONFIG.walk_roots`'s own note below). R-11 (M5, the purity corpus
    # milestone) owns deciding whether that prefix entry should be removed
    # outright instead of staying a documented no-op.
    path_prefixes=(("spine", "frames"), ("pipelines",), ("tests", "exemplar")),
    banned_bare_calls=_BANNED_BARE_CALLS,
    banned_attr_calls=_DATETIME_UUID_ATTR_CALLS,
    banned_attr_names=_SPARK_BANNED_ATTR_NAMES,
    banned_import_roots=_FRAMES_TRANSFORMS_BANNED_IMPORT_ROOTS,
    string_sql_sinks=_STRING_SQL_SINKS,
)

# "spine/effects/** + spine/stages/**: idiom rules plus a string-SQL review
# rule" (§12.3) — idiom-class applies to all of spine/** already (engine-wide
# below); this profile only carries the string-SQL sink names (shared with
# `_FRAMES_TRANSFORMS_PROFILE` above, see `_STRING_SQL_SINKS`'s own note).
_EFFECTS_STAGES_PROFILE = purity_linter.ScopeProfile(
    name="effects-stages",
    path_prefixes=(("spine", "effects"), ("spine", "stages")),
    string_sql_sinks=_STRING_SQL_SINKS,
)

# I-14: "no awsglue anywhere in spine/" — a package-wide rule, not scoped to
# any one subtree. Fixing critique F3(b): before this profile existed, only
# `_CORE_PROFILE` and `_FRAMES_TRANSFORMS_PROFILE` carried an `awsglue`
# import ban, leaving `spine/effects/**`, `spine/stages/**`,
# `spine/entrypoints/**`, `spine/bootstrap/**`, and every top-level
# `spine/*.py` module (`run.py`, `binding.py`, `config.py`, `context.py`,
# `observability.py`) with NO linter enforcement at all (only
# `entrypoints/glue_main.py` and `entrypoints/router.py` had ad hoc AST
# tests covering this — see `spine/tests/unit/test_glue_main_entrypoint.py`
# / `test_router_zip_purity.py` — nothing generalized it). A single profile
# matching BOTH of `CONFIG.walk_roots`' top-level segments
# (`path_prefixes=(("spine",), ("pipelines",))`) is a package-wide match by
# construction — `_matching_profiles` compares `rel_path`'s first path
# segment(s) against each prefix tuple, so a one-segment prefix matches
# every file under that root regardless of depth. `banned_import_roots` is
# the ONLY field this profile sets — it composes additively with
# `_CORE_PROFILE`/`_FRAMES_TRANSFORMS_PROFILE`/`_EFFECTS_STAGES_PROFILE`
# (each file's `_matching_profiles` returns every profile whose prefix
# matches; `_import_violations` checks each matching profile's
# `banned_import_roots` independently), replacing the two profiles' own
# `awsglue` entries rather than duplicating the ban three ways.
_PACKAGE_WIDE_PROFILE = purity_linter.ScopeProfile(
    name="package-wide",
    path_prefixes=(("spine",), ("pipelines",)),
    banned_import_roots=("awsglue",),
)

# Same allowance ingestion's config carries (LLD §6 preamble: pydantic
# `ValidationError` may propagate from boundary parses; a `field_validator`/
# `model_validator`-decorated method's `raise ValueError(...)` is exempt from
# `purity-raise` regardless of scope — `spine/core/model.py` (M1, bead
# conveyer-nvh.13) needs this the same way `ingestion/core/model.py` does.
# `try` remains banned everywhere in the core profile, including inside
# validator bodies — this exemption is raise-only, matching the engine's
# `_validator_decorated_raise_ids` (try/raise asymmetry is by design).
_VALIDATOR_DECORATOR_NAMES: frozenset[str] = frozenset(
    {"field_validator", "model_validator"}
)

# TransientError exempted by config as in ingestion (§12.3 last bullet).
_CLASS_SHAPE_ALLOWLIST: frozenset[tuple[str, str]] = frozenset(
    {
        ("spine/effects/records.py", "TransientError"),
    }
)

# The rendered MERGE in effects/spark.py is the single hardcoded exemption
# from the string-SQL rule, by (file, function) — the ingestion exemption
# mechanism (§12.3). Function name is provisional pending §7.6's effects/
# spark.py (M2); update to match once authored.
#
# 005.1 N1 (bead conveyer-azr.14, §12.6 item 2) addition —
# `frames/checks.py::_typed_expr` (§6.2): the ONE cast expression per
# declared column, reused by both the castability check and the typed
# projection (D-5's "no second cast to disagree"). PySpark 3.5's `Column`
# API has no `.try_cast()` method (arrives in 4.0), so numeric/bool
# typed-exprs are built via `F.expr(f"try_cast({quoted_name} AS {type})")`
# — expr text composed ONLY from the grammar-validated column name
# (backticks doubled, `core/merge.py::quote_identifier`'s rule) and the
# compiler's own rendering of the parsed `ColumnType` (never authored/
# free-form text) — the §6.7-merge-render precedent this bead follows.
_STRING_SQL_EXEMPTION: frozenset[tuple[str, str]] = frozenset(
    {
        ("spine/effects/spark.py", "render_merge"),
        ("spine/frames/checks.py", "_typed_expr"),
    }
)

CONFIG = purity_linter.LinterConfig(
    name="spine",
    # `"pipelines"` added by bead conveyer-nvh.22 (M3): `spine/pipelines/**`
    # (the identity exemplar's real home, D-2) was never walked before this
    # bead populated it — additive, `_discover_files` already tolerated a
    # missing/empty root the same way it does for `"spine"` pre-M1.
    # `"tests/exemplar"` deliberately NOT added here (see the
    # frames-transforms profile's own note on that path_prefix entry): post-
    # D-2 that directory holds no pure transform code, only `pipeline.yaml` /
    # fixture CSVs / ordinary pytest test files, and those legitimately use
    # `pathlib`/`open` the transforms profile bans — walking it would flag
    # test I/O, not a real purity defect.
    walk_roots=("spine", "pipelines"),
    # the `spine` uv-workspace module directory (`conveyer/spine/`, LLD §4),
    # NOT the repo root — see LinterConfig's docstring. Doesn't exist yet
    # (M1+); `_discover_files` tolerates the missing directory.
    package_root="spine",
    profiles=(
        _CORE_PROFILE,
        _FRAMES_TRANSFORMS_PROFILE,
        _EFFECTS_STAGES_PROFILE,
        _PACKAGE_WIDE_PROFILE,
    ),
    try_raise_allowlist=_TRY_RAISE_ALLOWLIST,
    validator_decorator_names=_VALIDATOR_DECORATOR_NAMES,
    class_shape_allowlist=_CLASS_SHAPE_ALLOWLIST,
    string_sql_exemption=_STRING_SQL_EXEMPTION,
)
