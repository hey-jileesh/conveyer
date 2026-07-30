"""Unit tests for `spine.entrypoints.glue_main` — LLD §8.3, I-14, I-22, I-23,
[H-5][T-13][T-16], 005.1 §3.2 [DC-4]/§3.5/A-11/§6.1's pinned obligation #1.

Covers every pure helper (`check_spec_uri_allowlist`, `_assert_binding_matches`,
`_catalog_conf`, `_assert_iceberg_extensions_active`, `default_fetch_spec`) in
isolation, plus `main()`-level fail-fast ordering: every defect class raises
BEFORE the next effect in the §8.3 sequence runs — proven either by a double
that raises if called (`fetch_spec`, for pre-fetch defects) or by a
deliberately "poisoned" `RunnerConfig` field whose own distinctive error
would surface INSTEAD of the expected one if the code incorrectly proceeded
past the defect (for post-fetch, pre-session-build defects) — no real Spark
session anywhere in this file EXCEPT the `_assert_patterns_compile_in_jvm`/
`_assert_temporal_fmt_compiles_in_jvm`/`_assert_temporal_bounds_bind` tests
below (beads conveyer-azr.18 and conveyer-azr.26): all three functions
validate against a REAL driver-side JVM by design ([DC-4]/§6.1's pinned
obligation #1 — a faked `spark._jvm`/`DataFrame.collect()` would defeat the
point of the check), so they use the shared session-scoped `spark` fixture
(`tests/conftest.py`), same as `tests/integration/test_entrypoint.py`; every
other defect class in this file stays session-free.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import boto3
import pytest
import yaml
from moto import mock_aws
from pydantic import ValidationError
from spine import config as config_module
from spine.binding import bind_transforms
from spine.core import contract as core_contract
from spine.core.model import ColumnSpec, PipelineSpecModel, RawContractModel
from spine.core.naming import slug
from spine.entrypoints import glue_main

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# --- shared fixtures ----------------------------------------------------------

_FEED_ID = "feed/identity"
_DELIVERY_ID = str(uuid.UUID(int=1, version=4))
_BATCH_ID = str(uuid.UUID(int=1, version=5))
_RECEIVED_AT = datetime(2026, 1, 1, tzinfo=UTC)
_LANDING_BUCKET = "conveyer-test-landing"
_PIPELINE = "pipelines/identity"
_PIPELINE_SLUG = slug(_PIPELINE)  # "pipelines--identity"

# Canonical I-22 shape, mirroring `core/naming.py::_canonical_prefix`/
# `_format_received_at` for `_RECEIVED_AT` (UTC, microsecond-precision,
# dash-free ISO8601): "20260101T000000000000Z".
_VALID_OBJECT_URI = (
    f"s3://{_LANDING_BUCKET}/{_FEED_ID}/received_at=20260101T000000000000Z/"
    f"dl-{_DELIVERY_ID}/object_1.csv"
)

_VALID_SPEC_URI = f"s3://some-artifacts-bucket/spine/specs/{_PIPELINE_SLUG}/pipeline.yaml"


def _delivery_json(**overrides: object) -> str:
    base: dict[str, object] = dict(
        schema_version=1,
        feed_id=_FEED_ID,
        delivery_id=_DELIVERY_ID,
        batch_id=_BATCH_ID,
        delivery_key="statement.csv",
        content_hash="sha256:" + "a" * 64,
        size_bytes=10,
        object_uris=[_VALID_OBJECT_URI],
        received_at=_RECEIVED_AT.isoformat(),
        pipeline=_PIPELINE,
    )
    base.update(overrides)
    return json.dumps(base)


def _spec_yaml(**overrides: object) -> str:
    base: dict[str, object] = dict(
        pipeline=_PIPELINE,
        transforms_module="pipelines.identity.transforms",
        raw_table="lake.identity__raw",
        quarantine_table="lake.identity__quarantine",
        fact_table="lake.identity__facts",
        state_table="lake.identity__state",
        read={"dialect": {"format": "csv"}},
        raw_contract={"columns": [{"name": "domain_id", "required": True, "nullable": False}]},
        sla_minutes=480,
    )
    base.update(overrides)
    return yaml.safe_dump(base)


def _argv(
    *,
    delivery_json: str = "",
    pipeline_spec_uri: str = _VALID_SPEC_URI,
    sla_minutes: str = "480",
    catalog_kind: str = "hadoop",
    warehouse_uri: str | None = "/tmp/should-never-be-touched",
) -> list[str]:
    if not delivery_json:
        delivery_json = _delivery_json()
    argv = [
        "--conveyer-env", "test",
        "--conveyer-aws-region", "us-east-1",
        "--conveyer-catalog-kind", catalog_kind,
        "--conveyer-ledger-catalog-kind", "sql",
        "--conveyer-ledger-sql-uri", "sqlite:///:memory:",
        "--conveyer-spine-db", "spine_test_db",
        "--conveyer-run-ledger-table", "run_ledger",
        "--conveyer-event-bus", "test-bus",
        "--conveyer-landing-bucket", _LANDING_BUCKET,
        "--conveyer-pipeline-spec-uri", pipeline_spec_uri,
        "--conveyer-delivery", delivery_json,
        "--conveyer-sfn-retry-count", "0",
        "--conveyer-sfn-redrive-count", "0",
        "--conveyer-run-config", "{}",
        "--conveyer-sla-minutes", sla_minutes,
        "--JOB_RUN_ID", "jr_test123",
    ]  # fmt: skip
    if warehouse_uri is not None:
        argv += ["--conveyer-warehouse-uri", warehouse_uri]
    return argv


def _never_call(uri: str) -> str:
    raise AssertionError(f"fetch_spec must not be called for this defect class: {uri!r}")


def _never_build_fx(spark: object, config: object) -> object:
    raise AssertionError("fx_factory must not be called for this defect class")


# --- `check_spec_uri_allowlist` (I-23) ---------------------------------------


def test_check_spec_uri_allowlist_accepts_valid_s3_uri() -> None:
    glue_main.check_spec_uri_allowlist(
        "s3://bucket/spine/specs/pipelines--identity/pipeline.yaml", "pipelines--identity"
    )


def test_check_spec_uri_allowlist_accepts_valid_file_uri() -> None:
    glue_main.check_spec_uri_allowlist(
        "file:///tmp/x/spine/specs/pipelines--identity/pipeline.yaml", "pipelines--identity"
    )


def test_check_spec_uri_allowlist_rejects_bad_scheme() -> None:
    with pytest.raises(ValueError, match="s3://.*file://"):
        glue_main.check_spec_uri_allowlist(
            "http://bucket/spine/specs/pipelines--identity/pipeline.yaml", "pipelines--identity"
        )


@pytest.mark.parametrize(
    "uri",
    [
        "s3://bucket/spine/specs/../secrets/pipeline.yaml",
        "s3://bucket/spine/specs/pipelines--identity/../../etc/pipeline.yaml",
        "file:///tmp/../../etc/spine/specs/pipelines--identity/pipeline.yaml",
    ],
)
def test_check_spec_uri_allowlist_rejects_traversal(uri: str) -> None:
    with pytest.raises(ValueError, match="traversal"):
        glue_main.check_spec_uri_allowlist(uri, "pipelines--identity")


def test_check_spec_uri_allowlist_rejects_wrong_pipeline_slug() -> None:
    with pytest.raises(ValueError, match="pipelines--other"):
        glue_main.check_spec_uri_allowlist(
            "s3://bucket/spine/specs/pipelines--identity/pipeline.yaml", "pipelines--other"
        )


def test_check_spec_uri_allowlist_rejects_uri_outside_specs_root() -> None:
    with pytest.raises(ValueError, match="spine/specs"):
        glue_main.check_spec_uri_allowlist(
            "s3://bucket/other/root/pipelines--identity/pipeline.yaml", "pipelines--identity"
        )


@pytest.mark.parametrize(
    "uri",
    [
        "s3://bucket/spine/specs/pipelines--identity/",
        "s3://bucket/spine/specs/pipelines--identity",
    ],
)
def test_check_spec_uri_allowlist_rejects_missing_filename(uri: str) -> None:
    with pytest.raises(ValueError, match="spine/specs"):
        glue_main.check_spec_uri_allowlist(uri, "pipelines--identity")


# --- `_assert_binding_matches` ([H-5], binding defect) -----------------------


def test_assert_binding_matches_passes_when_consistent() -> None:
    config = config_module.from_args(_argv())
    spec = PipelineSpecModel(**yaml.safe_load(_spec_yaml()))
    glue_main._assert_binding_matches(spec, glue_main._parse_seed(config), config)


def test_assert_binding_matches_raises_on_pipeline_mismatch() -> None:
    config = config_module.from_args(_argv())
    spec = PipelineSpecModel(**yaml.safe_load(_spec_yaml(pipeline="pipelines/other")))
    with pytest.raises(ValueError, match="binding defect"):
        glue_main._assert_binding_matches(spec, glue_main._parse_seed(config), config)


def test_assert_binding_matches_raises_on_sla_mismatch() -> None:
    config = config_module.from_args(_argv(sla_minutes="480"))
    spec = PipelineSpecModel(**yaml.safe_load(_spec_yaml(sla_minutes=10)))
    with pytest.raises(ValueError, match=r"\[H-5\]"):
        glue_main._assert_binding_matches(spec, glue_main._parse_seed(config), config)


# --- `_catalog_conf` (§7.6 catalog wiring) ------------------------------------


def test_catalog_conf_glue_kind_sets_glue_catalog_type() -> None:
    config = config_module.from_args(_argv(catalog_kind="glue", warehouse_uri=None))
    conf = glue_main._catalog_conf(config)
    assert conf["spark.sql.catalog.spine_cat.type"] == "glue"
    assert "spark.sql.catalog.spine_cat.warehouse" not in conf
    assert conf["spark.sql.extensions"] == glue_main._ICEBERG_EXTENSIONS


def test_catalog_conf_hadoop_kind_sets_warehouse() -> None:
    config = config_module.from_args(_argv(catalog_kind="hadoop", warehouse_uri="/tmp/wh"))
    conf = glue_main._catalog_conf(config)
    assert conf["spark.sql.catalog.spine_cat.type"] == "hadoop"
    assert conf["spark.sql.catalog.spine_cat.warehouse"] == "/tmp/wh"


def test_catalog_conf_hadoop_kind_requires_warehouse_uri() -> None:
    config = config_module.from_args(_argv(catalog_kind="hadoop", warehouse_uri=None))
    with pytest.raises(ValueError, match="warehouse_uri"):
        glue_main._catalog_conf(config)


# --- `_assert_iceberg_extensions_active` ([T-16]) -----------------------------


class _FakeConf:
    def __init__(self, extensions: str) -> None:
        self._extensions = extensions

    def get(self, key: str, default: str = "") -> str:
        assert key == "spark.sql.extensions"
        return self._extensions or default


class _FakeSession:
    def __init__(self, extensions: str) -> None:
        self.conf = _FakeConf(extensions)


def test_assert_iceberg_extensions_active_passes_when_present() -> None:
    session = _FakeSession(glue_main._ICEBERG_EXTENSIONS)
    glue_main._assert_iceberg_extensions_active(session)  # type: ignore[arg-type]


def test_assert_iceberg_extensions_active_raises_when_absent() -> None:
    session = _FakeSession("")
    with pytest.raises(AssertionError, match=r"\[T-16\]"):
        glue_main._assert_iceberg_extensions_active(session)  # type: ignore[arg-type]


# --- `default_fetch_spec` (I-23) ----------------------------------------------


def test_default_fetch_spec_reads_file_uri(tmp_path: Path) -> None:
    spec_file = tmp_path / "pipeline.yaml"
    spec_file.write_text("pipeline: pipelines/identity\n")
    assert glue_main.default_fetch_spec(f"file://{spec_file}") == "pipeline: pipelines/identity\n"


def test_default_fetch_spec_reads_bare_local_path(tmp_path: Path) -> None:
    spec_file = tmp_path / "pipeline.yaml"
    spec_file.write_text("pipeline: pipelines/identity\n")
    assert glue_main.default_fetch_spec(str(spec_file)) == "pipeline: pipelines/identity\n"


def test_default_fetch_spec_reads_s3_uri_via_boto3() -> None:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="some-artifacts-bucket")
        client.put_object(
            Bucket="some-artifacts-bucket",
            Key="spine/specs/pipelines--identity/pipeline.yaml",
            Body=b"pipeline: pipelines/identity\n",
        )
        text = glue_main.default_fetch_spec(
            "s3://some-artifacts-bucket/spine/specs/pipelines--identity/pipeline.yaml"
        )
        assert text == "pipeline: pipelines/identity\n"


# --- `_seed_batch_context` version computation (005.1 A-11/§3.5) ------------


def test_seed_batch_context_computes_versions_once_from_spec() -> None:
    config = config_module.from_args(_argv())
    seed = glue_main._parse_seed(config)
    spec = PipelineSpecModel(**yaml.safe_load(_spec_yaml()))
    transforms = bind_transforms(spec)

    ctx = glue_main._seed_batch_context(seed, spec, config, transforms)

    assert ctx.read_spec_version == core_contract.read_spec_version(spec.read)
    assert ctx.check_version == core_contract.check_version(spec.raw_contract, spec.read)
    # sanity: two genuinely different hashes, both full-length sha256 hex
    assert ctx.read_spec_version != ctx.check_version
    assert len(ctx.read_spec_version) == 64
    assert len(ctx.check_version) == 64


def test_seed_batch_context_versions_differ_across_different_specs() -> None:
    config = config_module.from_args(_argv())
    seed = glue_main._parse_seed(config)
    transforms_a = bind_transforms(PipelineSpecModel(**yaml.safe_load(_spec_yaml())))
    spec_a = PipelineSpecModel(**yaml.safe_load(_spec_yaml()))
    spec_b = PipelineSpecModel(
        **yaml.safe_load(
            _spec_yaml(
                raw_contract={
                    "columns": [
                        {"name": "domain_id", "required": True, "nullable": False},
                        {"name": "extra_column"},
                    ]
                }
            )
        )
    )

    ctx_a = glue_main._seed_batch_context(seed, spec_a, config, transforms_a)
    ctx_b = glue_main._seed_batch_context(seed, spec_b, config, transforms_a)

    assert ctx_a.check_version != ctx_b.check_version
    assert ctx_a.read_spec_version == ctx_b.read_spec_version  # `read:` unchanged between them


# --- `_assert_patterns_compile_in_jvm` (005.1 §3.2 [DC-4]) -- real JVM -------


def test_assert_patterns_compile_in_jvm_accepts_a_valid_pattern(spark: SparkSession) -> None:
    contract = RawContractModel(columns=[ColumnSpec(name="code", pattern=r"[A-Z]{3}-\d+")])
    glue_main._assert_patterns_compile_in_jvm(spark, contract)  # must not raise


def test_assert_patterns_compile_in_jvm_skips_columns_with_no_pattern(spark: SparkSession) -> None:
    contract = RawContractModel(columns=[ColumnSpec(name="id")])
    glue_main._assert_patterns_compile_in_jvm(spark, contract)  # no-op, must not raise


def test_assert_patterns_compile_in_jvm_rejects_java_invalid_python_valid_pattern(
    spark: SparkSession,
) -> None:
    # `(?P<name>...)` is Python/PCRE-only named-group syntax -- Java spells
    # it `(?<name>...)`; `core/model.py`'s own best-effort `re.compile` typo
    # check happily accepts it at spec parse (it IS valid Python regex).
    contract = RawContractModel(columns=[ColumnSpec(name="code", pattern=r"(?P<name>foo)")])
    with pytest.raises(ValueError, match=r"\[DC-4\]"):
        glue_main._assert_patterns_compile_in_jvm(spark, contract)


# --- `_assert_temporal_fmt_compiles_in_jvm` (bead conveyer-azr.26, --------
# --- n3-fmt-probe fix) -- real JVM -------------------------------------------


def _date_contract(**column_kwargs: object) -> RawContractModel:
    return RawContractModel(
        columns=[ColumnSpec(name="d", type="date(yyyy-MM-dd)", **column_kwargs)]  # type: ignore[arg-type]
    )


def test_assert_temporal_fmt_compiles_in_jvm_accepts_a_valid_fmt_with_no_bounds(
    spark: SparkSession,
) -> None:
    glue_main._assert_temporal_fmt_compiles_in_jvm(spark, _date_contract())  # must not raise


def test_assert_temporal_fmt_compiles_in_jvm_skips_non_temporal_columns(
    spark: SparkSession,
) -> None:
    contract = RawContractModel(columns=[ColumnSpec(name="id")])
    glue_main._assert_temporal_fmt_compiles_in_jvm(spark, contract)  # no-op, must not raise


def test_assert_temporal_fmt_compiles_in_jvm_rejects_malformed_fmt_with_no_bounds(
    spark: SparkSession,
) -> None:
    # THE residual gap this bead closes: a malformed fmt SEQUENCE on a
    # column declaring NEITHER min NOR max previously surfaced only the
    # first time a real batch executed -- `_assert_temporal_bounds_bind` has
    # no bound literal to probe with on this column, so it never ran at all.
    contract = RawContractModel(columns=[ColumnSpec(name="t", type="timestamp(yyyy-MM-dd V)")])
    with pytest.raises(ValueError, match="admission-defect/malformed-fmt-sequence"):
        glue_main._assert_temporal_fmt_compiles_in_jvm(spark, contract)


def test_assert_temporal_fmt_compiles_in_jvm_rejects_malformed_fmt_with_bounds_present(
    spark: SparkSession,
) -> None:
    # sanity: the general probe still fires even when bounds ARE declared --
    # deliberately redundant with `_assert_temporal_bounds_bind`'s own
    # bound-literal probe, no regression there.
    contract = RawContractModel(
        columns=[ColumnSpec(name="t", type="timestamp(yyyy-MM-dd V)", min="2020-01-01T00:00:00")]
    )
    with pytest.raises(ValueError, match="admission-defect/malformed-fmt-sequence"):
        glue_main._assert_temporal_fmt_compiles_in_jvm(spark, contract)


# --- `_assert_temporal_bounds_bind` (005.1 §6.1's pinned obligation #1) -----
# --- -- real JVM -------------------------------------------------------------


def test_assert_temporal_bounds_bind_accepts_valid_bounds(spark: SparkSession) -> None:
    contract = _date_contract(min="2020-01-01", max="2020-12-31")
    glue_main._assert_temporal_bounds_bind(spark, contract)  # must not raise


def test_assert_temporal_bounds_bind_skips_columns_with_no_bounds(spark: SparkSession) -> None:
    glue_main._assert_temporal_bounds_bind(spark, _date_contract())  # no-op, must not raise


def test_assert_temporal_bounds_bind_rejects_min_greater_than_max(spark: SparkSession) -> None:
    # never checked at spec-parse time for temporals (core/model.py's own
    # recorded gap, unlike int|long|decimal) -- this is the check that closes it.
    contract = _date_contract(min="2020-12-31", max="2020-01-01")
    with pytest.raises(ValueError, match="pinned obligation #1"):
        glue_main._assert_temporal_bounds_bind(spark, contract)


def test_assert_temporal_bounds_bind_rejects_unparseable_bound(spark: SparkSession) -> None:
    contract = _date_contract(min="not-a-date")
    with pytest.raises(ValueError, match="does not"):
        glue_main._assert_temporal_bounds_bind(spark, contract)


def test_assert_temporal_bounds_bind_rejects_malformed_fmt_sequence(spark: SparkSession) -> None:
    # "V" (timezone id) needs EXACTLY count 2 ("VV") -- alphabet-valid (single
    # letter, in `core/model.py`'s coarse letter set) but structurally invalid;
    # `core/model.py`'s spec-parse-time check is count-blind and lets this
    # through, so it only surfaces once the JVM formatter is actually built --
    # here, at bind, not the first time a real batch executes.
    contract = RawContractModel(
        columns=[ColumnSpec(name="t", type="timestamp(yyyy-MM-dd V)", min="2020-01-01T00:00:00")]
    )
    with pytest.raises(ValueError, match="JVM formatter rejects"):
        glue_main._assert_temporal_bounds_bind(spark, contract)


# --- I-5 attempt_id fallback ordering (mechanism lives in `spine.config`, --
# already exhaustively covered by `test_config.py`; confirmed here at the
# level `glue_main.main()` actually depends on it) ---------------------------


def test_from_args_attempt_id_fallback_ordering_confirmed_for_entrypoint() -> None:
    argv = _argv()  # only --JOB_RUN_ID present -> fallback
    assert config_module.from_args(argv).attempt_id == "jr_test123"

    overridden = _argv() + ["--conveyer-attempt-id", "override-id"]
    assert config_module.from_args(overridden).attempt_id == "override-id"  # override wins


# --- `main()` fail-fast ordering: raises before the next effect -------------


def test_main_raises_before_fetch_spec_on_malformed_seed_json() -> None:
    argv = _argv(delivery_json="not json at all")
    with pytest.raises(ValidationError):  # I-10 boundary-parse fail-fast class
        glue_main.main(argv, fetch_spec=_never_call, fx_factory=_never_build_fx)  # type: ignore[arg-type]


def test_main_raises_before_fetch_spec_on_malformed_batch_id() -> None:
    argv = _argv(delivery_json=_delivery_json(batch_id="not-a-uuid"))
    with pytest.raises(ValidationError):
        glue_main.main(argv, fetch_spec=_never_call, fx_factory=_never_build_fx)  # type: ignore[arg-type]


def test_main_raises_before_fetch_spec_on_forged_object_uris() -> None:
    forged_uri = _VALID_OBJECT_URI.replace(_FEED_ID, "feed/some-other-feed")
    argv = _argv(delivery_json=_delivery_json(object_uris=[forged_uri]))
    with pytest.raises(ValueError, match="I-22"):
        glue_main.main(argv, fetch_spec=_never_call, fx_factory=_never_build_fx)  # type: ignore[arg-type]


def test_main_raises_before_fetch_spec_on_spec_uri_allowlist_violation() -> None:
    wrong_root_uri = "s3://bucket/some/other/root/pipeline.yaml"
    argv = _argv(pipeline_spec_uri=wrong_root_uri)
    with pytest.raises(ValueError, match="I-23"):
        glue_main.main(argv, fetch_spec=_never_call, fx_factory=_never_build_fx)  # type: ignore[arg-type]


def test_main_raises_on_pipeline_mismatch_before_session_build() -> None:
    # `warehouse_uri=None` + `catalog_kind="hadoop"` would raise a DIFFERENT,
    # distinctive error from `_catalog_conf` if `main()` incorrectly reached
    # `_build_session` -- proving the binding-defect raise happens first.
    argv = _argv(catalog_kind="hadoop", warehouse_uri=None)
    fetch_spec = lambda uri: _spec_yaml(pipeline="pipelines/other")  # noqa: E731
    with pytest.raises(ValueError, match="binding defect"):
        glue_main.main(argv, fetch_spec=fetch_spec, fx_factory=_never_build_fx)  # type: ignore[arg-type]


def test_main_raises_on_sla_mismatch_before_session_build() -> None:
    argv = _argv(catalog_kind="hadoop", warehouse_uri=None, sla_minutes="480")
    fetch_spec = lambda uri: _spec_yaml(sla_minutes=10)  # noqa: E731
    with pytest.raises(ValueError, match=r"\[H-5\]"):
        glue_main.main(argv, fetch_spec=fetch_spec, fx_factory=_never_build_fx)  # type: ignore[arg-type]


# --- `main()` wiring: the two new 005.1 bind-time asserts raise BEFORE ------
# --- `fx_factory` (still pre-land) -- real JVM, via the shared `spark` ------
# --- fixture (adopted by `_build_session`'s own `getOrCreate()`) -----------


def test_main_raises_via_jvm_pattern_check_before_fx_factory(spark: SparkSession) -> None:
    argv = _argv(catalog_kind="hadoop")  # warehouse_uri: real session is adopted, never touched
    fetch_spec = lambda uri: _spec_yaml(  # noqa: E731
        raw_contract={"columns": [{"name": "code", "pattern": r"(?P<name>foo)"}]}
    )
    with pytest.raises(ValueError, match=r"\[DC-4\]"):
        glue_main.main(argv, fetch_spec=fetch_spec, fx_factory=_never_build_fx)  # type: ignore[arg-type]


def test_main_raises_via_temporal_bounds_check_before_fx_factory(spark: SparkSession) -> None:
    argv = _argv(catalog_kind="hadoop")
    fetch_spec = lambda uri: _spec_yaml(  # noqa: E731
        raw_contract={
            "columns": [
                {
                    "name": "d",
                    "type": "date(yyyy-MM-dd)",
                    "min": "2020-12-31",
                    "max": "2020-01-01",
                }
            ]
        }
    )
    with pytest.raises(ValueError, match="pinned obligation #1"):
        glue_main.main(argv, fetch_spec=fetch_spec, fx_factory=_never_build_fx)  # type: ignore[arg-type]


def test_main_raises_via_temporal_fmt_check_before_fx_factory(spark: SparkSession) -> None:
    # bead conveyer-azr.26: a temporal column with a malformed fmt SEQUENCE
    # and NO min/max -- `_assert_temporal_bounds_bind` alone would never
    # even see this column (no bound literal to probe with); `main()` must
    # still raise before `fx_factory`, via the new general fmt probe.
    argv = _argv(catalog_kind="hadoop")
    fetch_spec = lambda uri: _spec_yaml(  # noqa: E731
        raw_contract={"columns": [{"name": "t", "type": "timestamp(yyyy-MM-dd V)"}]}
    )
    with pytest.raises(ValueError, match="admission-defect/malformed-fmt-sequence"):
        glue_main.main(argv, fetch_spec=fetch_spec, fx_factory=_never_build_fx)  # type: ignore[arg-type]


# --- I-14: no `awsglue` anywhere in this module -----------------------------
#
# `entrypoints/**` matches NO `tools/purity_linter.py` `ScopeProfile` (only
# `core/`, `frames-transforms`, `effects-stages` are scoped -- confirmed
# empirically: `uv run python tools/purity_linter.py spine` reports zero
# violations against this file regardless of what it imports). Unlike
# `router.py`'s stdlib+boto3-only ZIP (enforced by its own dedicated import
# test, not the linter -- see that bead's own recorded finding), this
# module ships inside the full wheel (I-23), so a heavy import isn't itself
# a defect -- only a literal `awsglue` import would be. This is a cheap,
# direct, belt-and-suspenders check for that one specific claim, recorded
# here since neither the linter config nor a router-style zip-import test
# covers this file.
def test_glue_main_never_imports_awsglue() -> None:
    import ast

    tree = ast.parse(Path(glue_main.__file__).read_text())
    imported_roots = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    assert "awsglue" not in imported_roots


def test_main_raises_on_binding_failure_before_session_build() -> None:
    # I-10: `bind_transforms` failure (missing module) also precedes session
    # build/fx assembly.
    argv = _argv(catalog_kind="hadoop", warehouse_uri=None)
    fetch_spec = lambda uri: _spec_yaml(  # noqa: E731
        transforms_module="pipelines.this_module_does_not_exist"
    )
    with pytest.raises(ImportError):
        glue_main.main(argv, fetch_spec=fetch_spec, fx_factory=_never_build_fx)  # type: ignore[arg-type]
