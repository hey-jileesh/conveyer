"""pydantic contracts: seed event, `PipelineSpec`, lifecycle events, `LineageStamp`,
the 005.1 admission grammar (`ReadSpecModel`/`RawContractModel`/`ColumnSpec`).
004.1 LLD §6; admission grammar per 005.1 LLD §3.1/§3.2.

All boundary contracts are pydantic v2 models (parse, don't validate — with
narrow types, §6 preamble): a field this design later trusts (a name, an id,
a URI) carries a pattern, a bound, or a shape check here, at parse time —
"it parsed as `str`" is not trust. Internal-only values that never cross a
serialization boundary are `@dataclass(frozen=True)` (§7.0 rule 1) —
`LineageStamp` (§7.5 [C-5]) is the one such value in this module.

**005.1 §3.1/§3.2 scope note (bead conveyer-azr.11, n0-models)**: this module
implements `DialectModel`/`ReadSpecModel`/`ColumnSpec`/`RawContractModel` and
every spec-parse-time validator §3.2 assigns to them. `core/contract.py::
parse_column_type` is the single interpreter of the column-type grammar
(D-5); this module's own type-aware validators (decimal bounds, temporal
fmt, `min`/`max`) call it rather than re-deriving type-string parsing a
second time.

**§3.4 `PipelineSpecModel` migration (A-12, bead conveyer-azr.13,
n0-spec-migration)**: `required_columns`/`read: dict[str, JsonValue]` (the
I-P1/I-P2 provisional fields) are deleted; `PipelineSpecModel` now carries
required `read: ReadSpecModel` and `raw_contract: RawContractModel` fields
(no defaults — 005.1 A-12: "a pipeline without an admission surface is not
a pipeline"), plus the two cross-model validators §3.2 assigns to
`PipelineSpecModel` parse rather than `RawContractModel`/`ReadSpecModel`
individually, because each needs BOTH surfaces in hand: `dialect.header is
False` with any `required: True` column is an unsatisfiable claim (A-4);
`dialect.header is True` with `nullable: False` but `required: False` is
incoherent under header-bearing contracts ([R2-2]) — vacuous, not checked,
under `header: False` (positional binding always provides the column).
`core/contract.py::read_spec_version`/`check_version` (§3.3, A-11) also
land with this bead, in `contract.py` rather than here, for the same
circular-import reason `COLUMN_TYPE_RE` does (see that module's docstring).

**`min`/`max` scope, spelled out ([R2-5b])**: §3.2's own text parenthesizes
the deep "parseable as the column's type" check as "(checked at compile)" —
i.e. authoritative parseability is frames/checks.py's job (§6.1, N1), via
the SAME `try_cast`/`to_date`/`to_timestamp` expression the cast check uses
(D-5's one-cast-semantics rule) — a second, independently-derived temporal
parser in pure Python here would reintroduce exactly the twin-interpreter
risk D-5 exists to close, and Java/Spark pattern letters (`fmt`) aren't
interpretable via `datetime.strptime` directives anyway. So: this module
does the *structural* spec-parse-time checks only — `min`/`max` legal only
on `int|long|decimal|date|timestamp` columns; for `int|long|decimal`
("plain numeric literals"), a genuine best-effort Python parse (mirroring
`pattern`'s best-effort `re.compile` typo check) that ALSO backs `min <=
max` ordering; for `date`/`timestamp`, only a non-empty-string check (no
ordering comparison attempted — comparing two Java-pattern-formatted
literals without a JVM would need a second fmt interpreter, which this
module deliberately does not build). Flagged for the frames-compiler bead's
attention: temporal `min <= max` ordering has no spec-parse-time check under
this scoping and must be enforced at compile time if 005.1 wants it caught
before a batch runs.
"""

import contextlib
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from spine.core.contract import COLUMN_TYPE_RE, parse_column_type
from spine.core.naming import BATCH_ID_RE, _check_pipeline_slug_grammar
from spine.core.naming import check_qualified_table as check_qualified_table

# --- §6.1 shared patterns ----------------------------------------------------
#
# `BATCH_ID_RE`, the pipeline-slug grammar, and `check_qualified_table` are
# now single-sourced in `core/naming.py` (stdlib-pure, critique F5, bead
# conveyer-nvh.43) and imported here rather than re-derived: `core/naming.py`
# importing FROM this module would break `entrypoints/router.py`'s stdlib+
# boto3-only zip-purity constraint (§7.1, I-8 — this module is
# pydantic-shaped), but the reverse direction (this module importing the
# stdlib-pure `core/naming.py`) carries no such constraint, so it is the
# correct place to remove the duplication. `check_qualified_table` stays
# re-exported from here (unchanged name/behavior) so `CoEffectDecl.table`'s
# validator, `PipelineSpecModel`'s four table-field validators, and
# `core/merge.py`'s existing `from spine.core.model import ...
# check_qualified_table` import all keep working unmodified -- the
# `import check_qualified_table as check_qualified_table` redundant-alias
# form (not a plain import) is required by mypy's own `no_implicit_reexport`
# (the `spine.core.*` strict override, pyproject.toml): a name merely
# imported, not locally defined, is otherwise not part of this module's
# public interface, and `core/merge.py`'s import of it would fail
# `attr-defined`. `BATCH_ID_RE`/`_check_pipeline_slug_grammar` need no such
# alias -- nothing outside this module imports them FROM here.

# UUIDv4, [H-4]
_DELIVERY_ID_RE = r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
_FEED_ID_RE = r"^[a-z0-9][a-z0-9-]*/[a-z0-9][a-z0-9-]*$"


# --- §6.1 Seed event — spine-side `DeliveryRegisteredV1` --------------------


class DeliveryRegisteredV1(BaseModel):  # parse of SFN execution input
    model_config = ConfigDict(extra="allow")  # tolerant reader; unknown fields ignored
    schema_version: Literal[1]
    feed_id: str = Field(pattern=_FEED_ID_RE)
    delivery_id: str = Field(
        pattern=_DELIVERY_ID_RE
    )  # participates in the I-22 URI/name composition, so it is narrow-typed like batch_id [H-4]
    batch_id: str = Field(pattern=BATCH_ID_RE)
    delivery_key: str
    content_hash: str  # OPAQUE lineage here (004 D-13); never parsed
    size_bytes: int
    object_uris: list[str] = Field(min_length=1, max_length=256)  # each <=1024 chars, I-22
    received_at: AwareDatetime
    pipeline: str  # slug grammar re-checked at parse

    @field_validator("object_uris")
    @classmethod
    def _check_object_uri_lengths(cls, value: list[str]) -> list[str]:
        too_long = [uri for uri in value if len(uri) > 1024]
        if too_long:
            raise ValueError(
                f"object_uris entries must each be <= 1024 chars: {len(too_long)} over"
            )
        return value

    @field_validator("pipeline")
    @classmethod
    def _check_pipeline(cls, value: str) -> str:
        return _check_pipeline_slug_grammar(value)


# --- 005.1 §3.1 `ReadSpecModel` — the spec key stays `read:` ----------------


def _check_single_ascii_printable(value: str, field_name: str) -> str:
    """Shared by `DialectModel.delimiter`/`.quote` (§3.1): exactly one ASCII
    printable character. `str.isprintable()` already excludes control chars
    (incl. tab/DEL) while accepting the space character; `str.isascii()`
    excludes everything past the ASCII range -- the combination is exactly
    "ASCII printable" (probe-verified in the kernel, bead conveyer-azr.11)."""
    if len(value) != 1 or not value.isascii() or not value.isprintable():
        raise ValueError(f"{field_name} must be exactly one ASCII printable character: {value!r}")
    return value


class DialectModel(BaseModel):
    model_config = ConfigDict(extra="forbid")
    format: Literal["csv"]  # fixed-width/jsonl/ebcdic: reserved (005 §5.5)
    delimiter: str = ","  # exactly 1 char, ASCII printable, != quote
    quote: str = '"'  # exactly 1 char, ASCII printable
    header: bool = True
    multiline: bool = False  # true = declared §5.4 trade-off; §5.5 path

    @field_validator("delimiter")
    @classmethod
    def _check_delimiter(cls, value: str) -> str:
        return _check_single_ascii_printable(value, "delimiter")

    @field_validator("quote")
    @classmethod
    def _check_quote(cls, value: str) -> str:
        return _check_single_ascii_printable(value, "quote")

    @model_validator(mode="after")
    def _check_delimiter_quote_distinct(self) -> "DialectModel":
        if self.delimiter == self.quote:
            raise ValueError(f"delimiter and quote must differ: both are {self.delimiter!r}")
        return self


class ReadSpecModel(BaseModel):
    model_config = ConfigDict(extra="forbid")
    compression: Literal["none", "gzip", "zstd"] = "none"  # zstd reserved (A-2)
    charset: Literal["utf-8"] = "utf-8"  # other charsets: reserved values (A-2)
    dialect: DialectModel
    skip_leading_lines: int = Field(default=0, ge=0, le=4096)

    @field_validator("compression")
    @classmethod
    def _check_compression(cls, value: str) -> str:
        # A-10's tier-1 message grammar (`admission-defect/<code>: <machine
        # detail>`), raised at spec-parse (a "reserved-ladder-value"
        # binding defect, §5.7) -- the Literal type admits "zstd" precisely
        # so this validator can name the gap rather than letting a generic
        # pydantic literal-violation message stand in for it (A-2).
        if value == "zstd":
            raise ValueError(
                f"admission-defect/reserved-ladder-value: compression={value!r} is a "
                "reserved grammar value (005.1 A-2) -- not implemented in phase 1"
            )
        return value


# --- 005.1 §3.2 `RawContractModel` and the column grammar (005 §7.1, hardened) --

COLUMN_NAME_RE = r"^[A-Za-z_][A-Za-z0-9_]*$"  # fullmatch; also the Iceberg-safe set
# COLUMN_TYPE_RE lives in `core/contract.py` (imported above), not here --
# see that module's docstring for why (a circular-import fix: this class's
# own validators below need `parse_column_type`, which needs the regex; the
# regex cannot round-trip back through an import of this not-yet-fully-
# loaded module). `ColumnSpec.type`'s `Field(pattern=...)` still enforces
# the identical grammar via the imported name.

# §3.2: "date/timestamp fmt non-empty and drawn from the Spark datetime
# pattern alphabet (pure string check; full validity re-fails at bind,
# §6.1)". This is Spark SQL's documented datetime-pattern symbol alphabet
# (sql-ref-datetime-pattern), letters only -- the punctuation symbols (`'`
# quoting, `[`/`]` optional sections, the `#`/`{`/`}` reserved-for-future
# marks) are handled separately below, not folded into this set. A coarse,
# best-effort typo check, same posture as `pattern`'s `re.compile` check:
# the JVM (`to_date`/`to_timestamp`, §6.2) is the authoritative parser, at
# bind time -- not this module.
#
# **Empirically corrected (005.1 N1, bead conveyer-azr.14, pinned obligation
# #2)**: this set was originally written from memory against Spark's docs and
# was WRONG -- nine letters that Spark 3.5's real `to_date`/`to_timestamp`
# parser (under the §6.2-pinned `timeParserPolicy=CORRECTED`) rejects
# UNCONDITIONALLY, at every repetition count, were included: `Y` (week-based
# year), `q`/`Q` (quarter -- "Illegal pattern character" even for
# `to_date`/`to_timestamp` specifically, despite being documented for
# `date_format`'s output direction), `w`/`W`/`E`/`u`/`F` (week-based fields --
# Spark's own `SparkUpgradeException [INCONSISTENT_BEHAVIOR_CROSS_VERSION.
# DATETIME_PATTERN_RECOGNITION]`, "week-based patterns are unsupported since
# Spark 3.0"), and `p` ("Illegal pattern character" -- the java.time pad-next
# modifier, unsupported standalone by Spark's own pattern parser). Verified
# via `DataFrame.explain(True)` (forces eager format-string validation,
# driver-only, no `.collect()`/row execution needed -- probe-confirmed
# against Spark 3.5.x/3.5.x-compatible local substrate, bead conveyer-azr.14)
# at repetition counts 1-4 for every letter in the ORIGINAL set; every letter
# now excluded raised at EVERY count tried, with zero valid usage found. The
# remaining 20 letters (`GyMLdDahKkHmsSVzOXxZ`) each have at least one
# repetition count that Spark accepts (count-sensitivity, e.g. `V` needs
# exactly `VV`, is a SEPARATE concern this coarse, count-blind alphabet check
# was never meant to catch -- see this function's own docstring). A-15
# (`spine/tests/integration/test_engine_semantics.py`) pins a sample of both
# the removed and the kept letters as an executable regression, so a future
# Spark upgrade that shifts this alphabet again fails CI here, not silently.
_SPARK_DATETIME_PATTERN_LETTERS = frozenset("GyMLdDahKkHmsSVzOXxZ")

# A quoted literal section: `'...'`, `''` inside representing a literal `'`.
# Content inside is verbatim text, not pattern letters, and is excluded from
# the alphabet check below.
_QUOTED_LITERAL_RE = re.compile(r"'(?:[^']|'')*'")


def _datetime_fmt_alphabet_ok(fmt: str) -> bool:
    """True iff `fmt` is non-empty and every unquoted letter belongs to
    `_SPARK_DATETIME_PATTERN_LETTERS`. Also rejects a leftover unmatched `'`
    after stripping well-formed quoted sections (an unterminated literal is
    itself a spec defect this pure-string check can catch for free)."""
    if fmt.strip() == "":
        return False
    unquoted = _QUOTED_LITERAL_RE.sub("", fmt)
    if "'" in unquoted:
        return False
    return all(not c.isalpha() or c in _SPARK_DATETIME_PATTERN_LETTERS for c in unquoted)


def _is_compilable_regex(pattern: str) -> bool:
    """Pure classifier: does `pattern` compile as a Python regex? Mirrors
    `ingestion/core/model.py::_is_compilable_regex` -- `contextlib.suppress`
    (a `with` statement, not `ast.Try`) keeps this out of the core profile's
    try/raise ban without an allowlist entry. This is `pattern`'s
    best-effort TYPO check only -- `pattern`'s normative grammar is Java
    regex (the executing engine, [DC-4]); the JVM compile is bind-time,
    built in n3-context-wiring, not here."""
    compiled: re.Pattern[str] | None = None
    with contextlib.suppress(re.error):
        compiled = re.compile(pattern)
    return compiled is not None


def _parse_plain_int(value: str) -> int | None:
    """Best-effort "plain numeric literal" parse for `min`/`max` on an
    `int`/`long` column ([R2-5b]) -- `None` on failure. Rejects `_`-grouped
    literals (`"1_000"`), a Python-only syntax Spark's `try_cast` does not
    understand, even though bare `int()` would otherwise accept it."""
    if "_" in value:
        return None
    parsed: int | None = None
    with contextlib.suppress(ValueError):
        parsed = int(value)
    return parsed


def _parse_plain_decimal(value: str) -> Decimal | None:
    """Best-effort "plain numeric literal" parse for `min`/`max` on a
    `decimal` column ([R2-5b]) -- `None` on failure. Rejects `_`-grouped
    literals (see `_parse_plain_int`) and non-finite values (`Decimal`
    parses `"NaN"`/`"Infinity"` successfully, which are not numeric
    literals and would make a `min <= max` comparison meaningless -- NaN
    compares unequal to everything, silently admitting a nonsensical pair)."""
    if "_" in value:
        return None
    parsed: Decimal | None = None
    with contextlib.suppress(InvalidOperation, ValueError):
        parsed = Decimal(value)
    if parsed is not None and not parsed.is_finite():
        return None
    return parsed


_MIN_MAX_ELIGIBLE_KINDS = frozenset({"int", "long", "decimal", "date", "timestamp"})


class ColumnSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(pattern=COLUMN_NAME_RE, max_length=128)
    type: str = Field(default="string", pattern=COLUMN_TYPE_RE)
    required: bool = False  # header claim — tier 1 (005 §7.1)
    nullable: bool = True  # cell claim — tier 3
    allowed_values: list[str] | None = None  # compared on the raw string (§6.1)
    pattern: str | None = None  # fullmatch on the raw string (§6.1)
    min: str | None = None  # bound in the column's own type, as a string
    max: str | None = None  #   literal; cast at compile time (§6.1)

    @field_validator("pattern")
    @classmethod
    def _check_pattern_compiles(cls, value: str | None) -> str | None:
        if value is not None and not _is_compilable_regex(value):
            raise ValueError(
                f"pattern must be a syntactically valid regex (best-effort typo "
                f"check only -- normative grammar is Java regex, [DC-4]): {value!r}"
            )
        return value

    @field_validator("allowed_values")
    @classmethod
    def _check_allowed_values(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return value
        if len(value) == 0:
            raise ValueError("allowed_values must be non-empty when present")
        too_long = [v for v in value if len(v) > 1024]
        if too_long:
            raise ValueError(
                f"allowed_values entries must each be <= 1024 chars: {len(too_long)} over"
            )
        return value

    @model_validator(mode="after")
    def _check_type_semantics(self) -> "ColumnSpec":
        # `core/contract.py::parse_column_type` is the single interpreter of
        # the type grammar (D-5) -- reused here rather than re-deriving
        # decimal/date/timestamp parsing a second time.
        parsed = parse_column_type(self.type)
        if parsed.kind == "decimal":
            assert parsed.precision is not None and parsed.scale is not None, (
                "decimal kind always carries precision/scale (parse_column_type's own contract)"
            )
            if parsed.scale > parsed.precision:
                raise ValueError(
                    f"decimal scale must be <= precision for column {self.name!r}: {self.type!r}"
                )
            if parsed.precision > 38:
                raise ValueError(
                    "decimal precision must be <= 38 (Spark's ceiling, [DC-10]) for "
                    f"column {self.name!r}: {self.type!r}"
                )
        elif parsed.kind in ("date", "timestamp"):
            assert parsed.fmt is not None, (
                "date/timestamp kind always carries fmt (parse_column_type's own contract)"
            )
            if not _datetime_fmt_alphabet_ok(parsed.fmt):
                raise ValueError(
                    f"{parsed.kind} fmt must be non-empty and drawn from the Spark "
                    f"datetime pattern alphabet for column {self.name!r}: {parsed.fmt!r}"
                )
        return self

    @model_validator(mode="after")
    def _check_min_max(self) -> "ColumnSpec":
        if self.min is None and self.max is None:
            return self
        parsed = parse_column_type(self.type)
        if parsed.kind not in _MIN_MAX_ELIGIBLE_KINDS:
            raise ValueError(
                "min/max only allowed on int|long|decimal|date|timestamp columns: "
                f"column {self.name!r} declares type {self.type!r}"
            )
        # Structural spec-parse-time checks only -- see this module's
        # docstring ("min/max scope, spelled out") for why the deep
        # temporal parse is NOT attempted here.
        numeric: dict[str, int | Decimal] = {}
        for label, raw in (("min", self.min), ("max", self.max)):
            if raw is None:
                continue
            if parsed.kind in ("int", "long"):
                int_value = _parse_plain_int(raw)
                if int_value is None:
                    raise ValueError(
                        f"{label} must be a plain integer literal for column "
                        f"{self.name!r} (type {self.type!r}): {raw!r}"
                    )
                numeric[label] = int_value
            elif parsed.kind == "decimal":
                decimal_value = _parse_plain_decimal(raw)
                if decimal_value is None:
                    raise ValueError(
                        f"{label} must be a plain decimal literal for column "
                        f"{self.name!r} (type {self.type!r}): {raw!r}"
                    )
                numeric[label] = decimal_value
            else:  # date | timestamp -- full parse deferred to compile ([R2-5b])
                if raw.strip() == "":
                    raise ValueError(
                        f"{label} must be non-empty for column {self.name!r} (type {self.type!r})"
                    )
        if "min" in numeric and "max" in numeric and numeric["min"] > numeric["max"]:
            raise ValueError(
                f"min must be <= max for column {self.name!r}: min={self.min!r} max={self.max!r}"
            )
        return self


# The ten framework raw columns (§3.2 [DC-6], §4.1's DDL) -- a contract
# column may not collide with any of these, nor with the reserved
# `_conveyer_` prefix: a collision would produce duplicate-column DDL at
# bootstrap and a key collision in §7.1's snapshot object, breaking
# `row_hash` injectivity.
FRAMEWORK_RAW_COLUMNS = frozenset(
    {
        "batch_id",
        "delivery_id",
        "feed_id",
        "received_at",
        "source_uri",
        "object_seq",
        "row_index",
        "read_spec_version",
        "malformed_text",
        "extras",
    }
)
_RESERVED_COLUMN_PREFIX = "_conveyer_"


class RawContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid")
    columns: list[ColumnSpec] = Field(min_length=1, max_length=1024)
    forbid_replacement_chars: bool = True  # 005 §5.3; opt-out is a reviewed contract edit

    @model_validator(mode="after")
    def _check_columns(self) -> "RawContractModel":
        names = [c.name for c in self.columns]
        seen: set[str] = set()
        duplicated: set[str] = set()
        for name in names:
            if name in seen:
                duplicated.add(name)
            seen.add(name)
        if duplicated:
            raise ValueError(f"column names must be unique: duplicated {sorted(duplicated)!r}")
        framework_collisions = sorted(name for name in names if name in FRAMEWORK_RAW_COLUMNS)
        if framework_collisions:
            raise ValueError(
                "column names must be disjoint from the framework raw columns "
                f"([DC-6]): {framework_collisions!r}"
            )
        reserved_collisions = sorted(
            name for name in names if name.startswith(_RESERVED_COLUMN_PREFIX)
        )
        if reserved_collisions:
            raise ValueError(
                f"column names must not use the reserved prefix {_RESERVED_COLUMN_PREFIX!r} "
                f"([DC-6]): {reserved_collisions!r}"
            )
        return self


# --- §6.2 `PipelineSpec` — what the runner consumes -------------------------


class CoEffectDecl(BaseModel):
    model_config = ConfigDict(extra="forbid")
    table: str  # bare "<db>.<table>"; identifier-grammar checked
    own_state: bool = False  # self-reference flag — 004 §7.3 obligation to 006;
    # Phase 1: WARNING when true and serialize is false

    @field_validator("table")
    @classmethod
    def _check_table(cls, value: str) -> str:
        return check_qualified_table(value)


class PipelineSpecModel(BaseModel):
    model_config = ConfigDict(extra="forbid")
    pipeline: str  # must equal seed.pipeline, else binding defect
    transforms_module: str = Field(pattern=r"^pipelines\.[a-z0-9_]+(\.[a-z0-9_]+)*$")  # I-10
    co_effects: dict[str, CoEffectDecl] = {}  # name -> declaration; the ONLY reads pull performs;
    # ALSO an IaC input -- grants generated from it (I-21)
    raw_table: str
    quarantine_table: str
    fact_table: str
    state_table: str
    fold: Literal["default-lww", "custom"] = "default-lww"
    serialize: bool = False  # declared, not honored in Phase 1 (004 §16.2)
    domain_id_col: str = "domain_id"
    read: ReadSpecModel  # 005.1 §3.4/A-12: D-2, authored with the contract, one home
    raw_contract: RawContractModel  # 005.1 §3.4/A-12: required, no default (A-12's letter)
    # per-ATTEMPT budget (I-18). Two-sources-of-truth guard [H-5]: the DEPLOYED
    # timeouts are Terraform-time values; until 009 derives both from one
    # authored source, the entrypoint asserts this field equals
    # RunnerConfig.sla_minutes (the TF-passed value) -- binding-defect class,
    # so a spec edit that silently changes nothing fails loudly instead.
    sla_minutes: int = 480

    @field_validator("pipeline")
    @classmethod
    def _check_pipeline(cls, value: str) -> str:
        return _check_pipeline_slug_grammar(value)

    @field_validator("raw_table", "quarantine_table", "fact_table", "state_table")
    @classmethod
    def _check_tables(cls, value: str) -> str:
        return check_qualified_table(value)

    # --- 005.1 §3.2's two cross-model rules (need BOTH `read` and
    # `raw_contract` in hand, so they land at `PipelineSpecModel` parse,
    # not on either submodel individually) ------------------------------

    @model_validator(mode="after")
    def _check_header_false_forbids_required_columns(self) -> "PipelineSpecModel":
        # A-4: a `header: false` contract declaring any `required` column is
        # a spec-parse defect -- the claim is unsatisfiable (positional
        # binding has no header row to be absent-from).
        if self.read.dialect.header is False:
            offending = sorted(c.name for c in self.raw_contract.columns if c.required)
            if offending:
                raise ValueError(
                    "raw_contract declares required:true column(s) under "
                    f"dialect.header: false (005.1 A-4, unsatisfiable claim): {offending!r}"
                )
        return self

    @model_validator(mode="after")
    def _check_header_true_nullable_false_requires_required(self) -> "PipelineSpecModel":
        # [R2-2]: under a header-bearing contract, `nullable: false` without
        # `required: true` is incoherent -- a column absent from the header
        # binds all-null (A-4), silently turning a header-level condition
        # into 100% tier-3 quarantine instead of a tier-1 defect. Vacuous
        # (not checked) under `header: false`: positional binding always
        # provides the column, so short rows are malformed, never
        # null-padded.
        if self.read.dialect.header is True:
            offending = sorted(
                c.name for c in self.raw_contract.columns if not c.nullable and not c.required
            )
            if offending:
                raise ValueError(
                    "raw_contract declares nullable:false without required:true under "
                    f"dialect.header: true (005.1 [R2-2]): {offending!r}"
                )
        return self


# --- §6.6 Lifecycle events (payloads = batch truth, I-19) -------------------


class BatchStartedV1(BaseModel):  # DetailType "batch-started"; source conveyer.spine
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1] = 1
    pipeline: str
    feed_id: str
    batch_id: str
    delivery_id: str
    raw_count: int  # from read-back (durable), not attempt scaffolding
    land_snapshot_id: int | None  # stamped-summary resolution; None after expiry
    started_at: AwareDatetime  # ATTEMPT-truth -- declared exception [H-1]: `fx.now()` has no
    # durable source, so a rerun's emission carries its own clock; consumers dedup on
    # batch_id, the timestamp is informational (§6.6).


class BatchCompletedV1(BaseModel):  # PROVISIONAL -- 008 owns and freezes the payload
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1] = 1
    pipeline: str
    feed_id: str
    batch_id: str
    delivery_id: str
    raw_count: int
    pre_quarantined: int
    post_quarantined: int  # read-back by (batch_id[, stage])
    fact_count: int  # count of committed_facts_df -- batch truth [E-1].
    # Named fact_count, NOT facts_appended: the ledger's facts_appended is an
    # ATTEMPT delta (0 on guard-skip); one identifier must not carry two
    # sourcings [H-1].
    fact_snapshot_id: int | None
    state_snapshot_id: int | None  # None: expiry / fold no-op
    completed_at: AwareDatetime  # ATTEMPT-truth -- see BatchStartedV1.started_at.


# --- §7.5 [C-5] `LineageStamp` — `frames/` receives lineage as a value ------


@dataclass(frozen=True)
class LineageStamp:
    batch_id: str
    delivery_id: str
    feed_id: str
    received_at: datetime
    source_uri: str | None = None
