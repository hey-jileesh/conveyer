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
  function)`, the ingestion exemption mechanism). **[DC2-2] (007.1 §9.2/§15,
  bead `conveyer-6pg.23`, B11-local):** a banned attribute name, `overwrite`
  — RB-2's "no state-table overwrite missing both `validate-from-snapshot-
  id` and `isolation-level=serializable`" — licensed in exactly ONE file by
  `banned_attr_exemption` (a `(rel_path, attr_name)` pair set, the per-file
  exemption mechanism `tools/purity_linter.py` gained THIS bead — it did not
  exist before): `spine/effects/rebuild.py`, the one blessed rebuild/swap
  module (the effects layer's sole owner of SQL rendering; that module
  renders no SQL overwrite of a state table either — `INSERT OVERWRITE` has
  no construction site in this codebase). Every other file under either
  path prefix reports `purity-banned-attr:overwrite` on any `.overwrite(`
  call. `[DS2-2]`: growth of this profile's `banned_attr_names` or of
  `banned_attr_exemption` carries 006.1 §16.8's [DS-4] approver pattern
  (platform data-architecture owner + security-gate countersign) — an
  exemption is a licensed hole in the construction, never an ordinary
  review.
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
#
# 006.1 B0 (bead conveyer-6pg.10, n-check-grammar) addition --
# `check_grammar.py::_parse_expression` needs `try` for the same
# defects-as-values reason as `parse_line`/`multiline_records`: sqlglot's
# `parse_one` raises `SqlglotError` (a `ParseError`/`TokenError` subclass)
# on malformed authored text, including a well-formed-looking but
# syntactically incomplete fragment (`"a AND"`, sqlglot's default error
# level raises for this immediately rather than returning a partial tree)
# -- `_parse_expression` converts that raise into a returned `None`, per
# `validate_expression`'s own totality contract (§6.4).
#
# 006.1 B0 (bead conveyer-6pg.10, n-check-grammar) addition --
# `model.py::parse_pipeline_spec_yaml` is a plain raise-only helper (not a
# `@field_validator`/`@model_validator`), same shape as `parse_column_type`:
# it raises a plain `ValueError` (`bind-defect/duplicate-key: ...`) itself
# on a genuine duplicate YAML mapping key (§4's strict-loader obligation,
# S1) -- no `try` involved, just a bare `raise`, exempt for the same
# raise-only-helper reason as this list's other `model.py` entries.
# `_check_id_not_reserved`/`_check_reason_not_reserved` are the SAME shape
# again: plain validator-SUPPORT helpers (K1/[AE-6], K6) shared across
# `RowCheckModel`/`MembershipCheckModel`/`BatchCheckModel`'s own
# `@field_validator`s, not themselves decorated -- `_check_pipeline_slug_
# grammar`'s own precedent, restated once more for the check-kind models.
#
# 006.1 B0 (bead conveyer-6pg.10, n-check-grammar) addition -- absorbs
# `conveyer-azr.25`: `naming.py::_format_received_at` gains the SAME
# arithmetic-pre-check-then-raise idiom `canonical.py::_timestamp_str`
# already carries (`conveyer-azr.24`) for the identical `OverflowError`
# root cause (an aware datetime at/near `datetime.min`/`.max` whose UTC
# conversion falls outside `[MINYEAR, MAXYEAR]`) -- a bare `raise
# ValueError(...)`, no `try`, same raise-only-helper shape as this list's
# other `naming.py` entries.
_TRY_RAISE_ALLOWLIST: frozenset[tuple[str, str]] = frozenset(
    {
        ("spine/core/model.py", "_check_pipeline_slug_grammar"),
        ("spine/core/model.py", "check_qualified_table"),
        ("spine/core/model.py", "_check_single_ascii_printable"),
        ("spine/core/model.py", "parse_pipeline_spec_yaml"),
        ("spine/core/model.py", "_check_id_not_reserved"),
        ("spine/core/model.py", "_check_reason_not_reserved"),
        ("spine/core/naming.py", "_check_pipeline_slug_grammar"),
        ("spine/core/naming.py", "_format_received_at"),
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
        ("spine/core/check_grammar.py", "_parse_expression"),
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
# WOULD trip this same rule the day one is genuinely needed. Historical
# note, now resolved (007.1 [DC2-2], bead `conveyer-6pg.23`, B11-local):
# this comment used to record that `banned_attr_names` had NO `(file,
# function)` exemption mechanism at all — that gap is what B11-local's
# `.overwrite(` ban (below, `_EFFECTS_STAGES_PROFILE`) needed closed FIRST,
# and did, in `tools/purity_linter.py`'s own engine (`LinterConfig.
# banned_attr_exemption`, a `(rel_path, attr_name)` pair set,
# `_attr_name_violations` now consults it). A real `F.count` aggregate under
# `frames/**` could use the identical mechanism the day one is genuinely
# needed — this ban itself is left as-is (no exemption entry exists for
# `count`, `frames/**` still bans it outright).
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

# [DC2-2] (007.1 §9.2/§15's final row, bead `conveyer-6pg.23`, B11-local):
# `.overwrite(` on a state table, missing either `validate-from-snapshot-id`
# or `isolation-level=serializable`, is the RB-2 "swap that blindly wins"
# hazard — empirically reconfirmed, this bead's own kernel probe: alone,
# `validate-from-snapshot-id` is silently ignored by the `OverwriteByFilter`
# path. `_attr_name_violations` cannot distinguish a well-formed two-option
# call from a bare one (it flags the ATTRIBUTE NAME alone, not its call
# arguments — the same best-effort AST posture every other `banned_attr_
# names` entry already accepts) — banning `overwrite` outright, everywhere
# under `spine/effects/**`/`spine/stages/**`, and licensing exactly ONE
# construction site via `_BANNED_ATTR_EXEMPTION` below is therefore §15's
# own "construction, named [DC2-2]" reading: the ONE place `.overwrite(`
# may appear at all is `spine/effects/rebuild.py::attempt_state_swap`
# (the effects layer's sole owner of SQL/write rendering, per that
# module's own docstring) — every other file under either path prefix
# reports `purity-banned-attr:overwrite` on ANY `.overwrite(` call,
# regardless of its options.
_STATE_OVERWRITE_BANNED_ATTR_NAMES: frozenset[str] = frozenset({"overwrite"})

# [DS2-2] governance note (007.1 §16.1 [DC2-2], carried verbatim in
# substance): growth of EITHER this profile's `banned_attr_names` OR of
# `_BANNED_ATTR_EXEMPTION` below is NOT an ordinary code-review change — an
# exemption is precisely a licensed hole in the construction §15's final
# row leans on to make RB-2's "no --force path" hold BY CONSTRUCTION, not
# by discipline. The same approver pattern 006.1 §16.8's [DS-4] names
# (platform data-architecture owner + security-gate countersign) applies to
# any PR touching either set — this comment IS that requirement's recorded
# site, not a substitute for actually routing the review.
_BANNED_ATTR_EXEMPTION: frozenset[tuple[str, str]] = frozenset(
    {
        ("spine/effects/rebuild.py", "overwrite"),
    }
)

# [DS2-2] annotation (2026-09-01, bead conveyer-6pg.33 -- erratum note per
# house convention, design/adr-oq5-batch-progress-grain.md:62's annotation
# shape adapted to this file's own comment form; the [DS2-2] governance
# note above stands unaltered): naming two residuals that note leaves
# implicit.
#
# **Path-scope residual.** The `.overwrite(` MECHANICAL ban covers only
# `_EFFECTS_STAGES_PROFILE`'s two path prefixes (`spine/effects/**`,
# `spine/stages/**`) -- `spine/bootstrap/**`/`spine/entrypoints/**` carry NO
# `banned_attr_names` entry for `overwrite` at all, and both directories
# legitimately use `spark.sql(...)` for real deploy-principal DDL (`CREATE
# TABLE`/`ALTER TABLE`/`SET TBLPROPERTIES` -- `bootstrap/create_record_
# tables.py`'s/`create_admission_tables.py`'s own account); a SQL `INSERT
# OVERWRITE` string authored there would not be caught by this linter at
# all. This is 007.1 §9.2's OWN "SQL INSERT OVERWRITE ... grep-shaped
# audit ... detection-in-depth" half of the rule (§9.2, the [DC2-2]
# construction row), applying VERBATIM -- never a mechanical AST check
# anywhere in this repo, by that section's own design, not an oversight
# introduced here; the risk is PRICED (deploy-time DDL under a different,
# out-of-band-reviewed principal), not silently accepted.
#
# **[DS-4] approver-pattern residual.** The "approver pattern ... applies"
# sentence in the note above is DISCIPLINE, stated plainly here -- a
# comment recording the requirement, not a mechanical CODEOWNERS route or
# any other CI-enforced gate. A PR touching `banned_attr_names`/
# `_BANNED_ATTR_EXEMPTION` above is not blocked by any tooling from merging
# without the named review; this comment is the audit trail for "was this
# reviewed," not a gate that makes skipping it impossible.

# "spine/effects/** + spine/stages/**: idiom rules plus a string-SQL review
# rule" (§12.3) — idiom-class applies to all of spine/** already (engine-wide
# below); this profile carries the string-SQL sink names (shared with
# `_FRAMES_TRANSFORMS_PROFILE` above, see `_STRING_SQL_SINKS`'s own note) plus
# (B11-local, [DC2-2]) the `.overwrite(` ban above.
_EFFECTS_STAGES_PROFILE = purity_linter.ScopeProfile(
    name="effects-stages",
    path_prefixes=(("spine", "effects"), ("spine", "stages")),
    string_sql_sinks=_STRING_SQL_SINKS,
    banned_attr_names=_STATE_OVERWRITE_BANNED_ATTR_NAMES,
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
#
# 006.1 §13.4 item 1 addition — `frames/business_checks.py::_compiled_expr`
# (§7.1): the ONE `F.expr(...)` call site business-check row expressions
# execute through, over `ValidatedExpr.authored_text` (byte-exact — the
# gatekeeper-accepted, gate-1-re-derived string, never the raw authored
# `RowCheckModel.expr` field, §6.4's executed-text rule) — the exact
# `_typed_expr` precedent, restated for the post_check interpreter's own
# string-SQL sink.
_STRING_SQL_EXEMPTION: frozenset[tuple[str, str]] = frozenset(
    {
        ("spine/effects/spark.py", "render_merge"),
        ("spine/frames/checks.py", "_typed_expr"),
        ("spine/frames/business_checks.py", "_compiled_expr"),
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
    banned_attr_exemption=_BANNED_ATTR_EXEMPTION,
)
