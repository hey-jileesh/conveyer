"""Session-building — the ONE authored source for catalog conf, `SparkSession`
build, and the [T-16] extensions-active probe every entrypoint needs.

**F2 fix (critique gate wf_78ea4599-a5b, bead conveyer-swb.25).** Before this
module existed, `entrypoints/glue_main.py` and `entrypoints/rebuild_main.py`
each hand-rolled their own `_catalog_conf`/`_build_session`/`_assert_iceberg_
extensions_active` under a "duplicate narrow, versionless shapes rather than
reach into another module's private surface" precedent (`effects/rebuild.py`'s
own, `entrypoints/rebuild_main.py`'s own module docstring). That precedent is
sound for a two-line snapshot-id read; it is NOT sound for a conf dict that
carries a correctness-load-bearing constant (`frames.checks.SESSION_PINS` --
005.1 §12.1/§6.2's three cast-semantics session pins), because the two copies
had already drifted on day one: `glue_main._catalog_conf` carried
`SESSION_PINS`, `rebuild_main._catalog_conf` omitted it entirely -- same code,
silently different engine semantics for a rebuild session vs. a batch
session, the "two evaluators" class this fix closes. `glue_main.py` and
`rebuild_main.py` now both import `catalog_conf`/`build_session`/`assert_
iceberg_extensions_active` from here and define no private copies of their
own -- one authored source, so they cannot drift apart again.

**`SessionConfig` (structural, mirrors `effects/ledger.py::LedgerConfig`'s
own M3 technique, this bead).** `catalog_conf`/`build_session` need exactly
TWO fields (`catalog_kind`, `warehouse_uri`) -- narrower than either
caller's own full config dataclass (`spine.config.RunnerConfig`'s 18 fields,
`entrypoints/rebuild_main.py::RebuildConfig`'s 11). Typing against this
`Protocol` rather than either concrete dataclass means BOTH satisfy it
structurally, with no adapter and no forced coupling to whichever caller
happens to be first: a rebuild invocation never needs a fake `RunnerConfig`
seed/SFN/SLA field just to build a session, and a batch invocation never
needs to depend on `RebuildConfig`.

**N3 fix (critique gate wf_a0ef7f3b-6aa, bead conveyer-swb.28): `env` was
declared on this Protocol but never genuinely read here.** Neither
`catalog_conf` nor `build_session` ever touches `config.env` -- the ONLY
`env`-derived value either function needs, `app_name`, is a caller-supplied
keyword (see immediately below), computed by `glue_main.py`/`rebuild_main.py`
themselves off their OWN concrete config's `env` field before `build_session`
is ever called. A Protocol one field wider than what its consumers read is
the exact class `effects/ledger.py::LedgerConfig`'s own M3 fix closed for
`LedgerConfig` -- closed here the same way, by narrowing the Protocol
itself rather than the docstring's claim.

**`app_name` is a caller-supplied keyword, not derived here.** The two
callers' app names differ (`f"conveyer-spine-{config.env}"` vs.
`f"conveyer-spine-rebuild-{config.env}"`, distinguishing the two run modes
in the Spark UI/driver logs) -- a cosmetic difference this module has no
opinion on, so it stays a parameter rather than a second config field or a
run-mode flag.

`build_session` builds (or, inside an already-active JVM such as the shared
test session, ADOPTS via `getOrCreate()`) the `SparkSession`: no
`.master(...)` is set here deliberately -- in production Glue/EMR already
provides the master via its own bootstrap (no `GlueContext`, I-14); in
tests, the shared session-scoped `spark` fixture (`tests/conftest.py`) has
already set `local[2]` before this function's `getOrCreate()` call ever
runs, so it adopts that same live session rather than conflicting with it.

`assert_iceberg_extensions_active` is [T-16]'s cheap conf-only probe
(`spark.conf.get("spark.sql.extensions")`), NOT `tests/conftest.py::
_assert_iceberg_extensions_live`'s live `CREATE TABLE` + `MERGE INTO`
round-trip -- deliberately different depths for different jobs (see that
function's own docstring below for the full account)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Final, Literal, Protocol

from spine.frames.checks import SESSION_PINS

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

ICEBERG_EXTENSIONS: Final[str] = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


class SessionConfig(Protocol):
    """The TWO fields `catalog_conf`/`build_session` genuinely read --
    both `spine.config.RunnerConfig` and `entrypoints/rebuild_main.py::
    RebuildConfig` satisfy this structurally, with no adapter (F2, this
    bead's own resolution -- mirrors `effects/ledger.py::LedgerConfig`'s
    identical M3 technique). Declared as read-only `@property` members
    (not plain annotations): both concrete configs are FROZEN dataclasses,
    whose fields mypy treats as read-only -- a plain-annotation `Protocol`
    member defaults to read-WRITE, which a frozen dataclass structurally
    fails (`expected settable variable, got read-only attribute`).

    No `env` member (N3 fix, bead conveyer-swb.28): neither function below
    reads it -- see the module docstring's N3 note."""

    @property
    def catalog_kind(self) -> Literal["glue", "hadoop"]: ...  # Spark data-path catalog (I-2)
    @property
    def warehouse_uri(self) -> str | None: ...  # hadoop only (tests)


def catalog_conf(config: SessionConfig) -> dict[str, str]:
    """§7.6 catalog wiring, as a plain conf dict (pure -- no `SparkSession`
    call): `spine_cat` -> Iceberg `SparkCatalog`, `type=glue` in production
    or `type=hadoop` (`warehouse=config.warehouse_uri`) in tests, chosen by
    `config.catalog_kind` (I-2). AQE on (§8.3). `build_session` is the one
    caller that turns this into a live session.

    Also carries 005.1 §12.1/§6.2's three cast-semantics session pins
    (`frames.checks.SESSION_PINS` -- the one authored source, `tests/
    conftest.py::_BASE_CONF` wires the SAME constant so they cannot drift
    apart, per that module's own docstring) -- for EVERY caller of this
    function, batch or rebuild alike (F2's own fix: this is exactly the
    constant `rebuild_main`'s own pre-fix copy omitted)."""
    conf = {
        "spark.sql.extensions": ICEBERG_EXTENSIONS,
        "spark.sql.catalog.spine_cat": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.adaptive.enabled": "true",
        **SESSION_PINS,
    }
    if config.catalog_kind == "glue":
        conf["spark.sql.catalog.spine_cat.type"] = "glue"
        return conf
    if not config.warehouse_uri:
        raise ValueError("warehouse_uri is required when catalog_kind='hadoop'")
    conf["spark.sql.catalog.spine_cat.type"] = "hadoop"
    conf["spark.sql.catalog.spine_cat.warehouse"] = config.warehouse_uri
    return conf


def build_session(config: SessionConfig, *, app_name: str) -> SparkSession:
    """Builds (or ADOPTS, inside an already-active JVM) the `SparkSession`
    wired to `catalog_conf`'s conf, under the caller-supplied `app_name`
    (module docstring: the two run modes' own app names differ, a cosmetic
    choice this module has no opinion on)."""
    from pyspark.sql import SparkSession  # local import: keeps this module's pure

    # helpers importable without a live JVM even being reachable; matches
    # `spine.binding`/`spine.context`'s own lazy-pyspark-import convention.
    builder = SparkSession.builder.appName(app_name)
    for key, value in catalog_conf(config).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def assert_iceberg_extensions_active(spark: SparkSession) -> None:
    """[T-16]: a cheap conf-only probe (`spark.conf.get("spark.sql.
    extensions")`), NOT `tests/conftest.py::_assert_iceberg_extensions_live`'s
    live `CREATE TABLE` + `MERGE INTO` round-trip. Deliberately different
    depths for different jobs: conftest's round-trip validates the local
    Iceberg JAR + catalog wiring itself, ONCE per CI session, before any test
    runs -- worth the cost there. Running a real `MERGE INTO` at every job
    START (this function's own call site, in EITHER run mode) to prove the
    SAME thing would burn a write against the target catalog on every
    attempt, for a property (`spark.sql.extensions` being correctly set)
    that a conf read already proves cheaply. Still fail-fast BEFORE any
    stage/swap ever runs: a missing extension raises here, not several
    steps in at the first real Iceberg action [T-16]."""
    extensions = spark.conf.get("spark.sql.extensions", "") or ""
    if ICEBERG_EXTENSIONS not in extensions:
        raise AssertionError(
            "Iceberg SQL extensions are not active on this SparkSession "
            f"(spark.sql.extensions={extensions!r}) -- a MERGE INTO/overwrite would fail later "
            "instead of here [T-16]"
        )
