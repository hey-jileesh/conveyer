"""LinterConfig selection tests (LLD 004.1 I-15/§12.3, bead conveyer-nvh.10):
both `tools/linter_configs/ingestion.py` and `tools/linter_configs/spine.py`
load as valid `LinterConfig` values, the ingestion config reproduces the
pre-promotion rule tables (captured below as literal expected values, from
git history of `ingestion/tools/purity_linter.py` before its removal)
exactly, and the CLI's config-name resolution (`main()`'s `_load_config`
path) picks each config up correctly.

`tools/` is not itself a package — see `tools/purity_linter.py`'s module
docstring — so it's put on `sys.path` explicitly, same as
`test_purity_linter.py`.
"""

from __future__ import annotations

import io
import sys
from contextlib import redirect_stdout
from pathlib import Path

_TOOLS_DIR = Path(__file__).resolve().parents[3] / "tools"
if str(_TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(_TOOLS_DIR))

import purity_linter  # noqa: E402  (path setup must precede this import)
from linter_configs import ingestion as ingestion_config  # noqa: E402
from linter_configs import spine as spine_config  # noqa: E402

# --- the pre-promotion rule tables, captured verbatim as expected values ---
# (previously module-level constants in `ingestion/tools/purity_linter.py`,
# removed by this same change; see git history for the pre-move file).

_EXPECTED_PURITY_BANNED_IMPORT_ROOTS: tuple[str, ...] = (
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

_EXPECTED_IDIOM_BANNED_IMPORT_ROOTS: tuple[str, ...] = (
    "toolz",
    "cytoolz",
    "returns",
    "pyrsistent",
    "funcy",
    "effect",
)

_EXPECTED_IDIOM_BANNED_IMPORT_DOTTED: tuple[str, ...] = ("unittest.mock",)

_EXPECTED_BANNED_BARE_CALLS: frozenset[str] = frozenset({"open", "eval", "exec", "__import__"})

_EXPECTED_BANNED_ATTR_CALLS: frozenset[tuple[str, str]] = frozenset(
    {
        ("datetime", "now"),
        ("datetime", "utcnow"),
        ("datetime", "today"),
        ("date", "today"),
        ("uuid", "uuid1"),
        ("uuid", "uuid4"),
    }
)

_EXPECTED_TRY_RAISE_ALLOWLIST: frozenset[tuple[str, str]] = frozenset(
    {
        ("ingestion/core/completeness.py", "parse_manifest"),
        ("ingestion/core/model.py", "_check_iana_timezone"),
    }
)

_EXPECTED_VALIDATOR_DECORATOR_NAMES: frozenset[str] = frozenset(
    {"field_validator", "model_validator"}
)

_EXPECTED_CLASS_SHAPE_ALLOWLIST: frozenset[tuple[str, str]] = frozenset(
    {
        ("ingestion/effects/records.py", "TransientError"),
    }
)


def test_ingestion_config_loads_as_linter_config() -> None:
    assert isinstance(ingestion_config.CONFIG, purity_linter.LinterConfig)
    assert ingestion_config.CONFIG.name == "ingestion"


def test_spine_config_loads_as_linter_config() -> None:
    assert isinstance(spine_config.CONFIG, purity_linter.LinterConfig)
    assert spine_config.CONFIG.name == "spine"


def test_ingestion_config_reproduces_prior_rule_tables_exactly() -> None:
    """VERBATIM transfer check (LLD I-15): every rule table that used to be
    a module-level constant in the pre-promotion linter is now data on
    `ingestion_config.CONFIG`, unchanged."""
    config = ingestion_config.CONFIG
    assert config.walk_roots == ("ingestion", "sources")
    assert config.idiom_banned_import_roots == _EXPECTED_IDIOM_BANNED_IMPORT_ROOTS
    assert config.idiom_banned_import_dotted == _EXPECTED_IDIOM_BANNED_IMPORT_DOTTED
    assert config.try_raise_allowlist == _EXPECTED_TRY_RAISE_ALLOWLIST
    assert config.validator_decorator_names == _EXPECTED_VALIDATOR_DECORATOR_NAMES
    assert config.class_shape_allowlist == _EXPECTED_CLASS_SHAPE_ALLOWLIST

    assert len(config.profiles) == 1
    purity_profile = config.profiles[0]
    assert purity_profile.path_prefixes == (("sources",), ("ingestion", "core"))
    assert purity_profile.banned_import_roots == _EXPECTED_PURITY_BANNED_IMPORT_ROOTS
    assert purity_profile.banned_bare_calls == _EXPECTED_BANNED_BARE_CALLS
    assert purity_profile.banned_attr_calls == _EXPECTED_BANNED_ATTR_CALLS
    assert purity_profile.ban_try_raise is True


def test_spine_config_declares_its_profiles() -> None:
    """§12.3 names three profiles (`core`, `frames-transforms`,
    `effects-stages`); a fourth, `package-wide`, was added post-M5 (design-
    critique finding F3(b), bead conveyer-nvh.42) to carry a single,
    non-duplicated `awsglue` import ban (I-14) across the whole of
    `spine/**` + `pipelines/**`, including subtrees (`effects/`, `stages/`,
    `entrypoints/`, `bootstrap/`, and spine's top-level modules) that the
    original three profiles' `path_prefixes` never reached."""
    names = sorted(profile.name for profile in spine_config.CONFIG.profiles)
    assert names == ["core", "effects-stages", "frames-transforms", "package-wide"]


def test_discover_files_tolerates_missing_walk_root(tmp_path: Path) -> None:
    """A `walk_roots` entry that isn't a directory is skipped, not errored --
    the same tolerance that let `spine.py`'s config load and be CLI-
    selectable before `spine/` existed (LLD 004.1 M0, bead conveyer-nvh.12
    made it real; the general mechanism is what's under test here now)."""
    config = purity_linter.LinterConfig(
        name="synthetic",
        walk_roots=("does-not-exist-yet",),
        package_root="",
        profiles=(),
    )
    files = purity_linter._discover_files(tmp_path, config)
    assert files == []


def test_spine_config_walks_the_real_spine_package() -> None:
    """`spine/` is real as of bead conveyer-nvh.12 (M0) -- its config now
    walks actual files instead of tolerating absence (see the general-
    mechanism test above for that behavior)."""
    repo_root = Path(__file__).resolve().parents[3]
    files = purity_linter._discover_files(
        repo_root / spine_config.CONFIG.package_root, spine_config.CONFIG
    )
    assert files != []
    assert all(f.suffix == ".py" for f in files)


def test_cli_selects_config_by_name_for_both_packages() -> None:
    """`main()`'s config-name resolution path (`python tools/purity_linter.py
    <config-name>`) finds both configs via `tools/linter_configs/`."""
    buf = io.StringIO()
    with redirect_stdout(buf):
        ingestion_exit = purity_linter.main(["ingestion"])
    assert ingestion_exit == 0
    assert buf.getvalue() == ""

    buf2 = io.StringIO()
    with redirect_stdout(buf2):
        spine_exit = purity_linter.main(["spine"])
    assert spine_exit == 0
    assert buf2.getvalue() == ""


def test_cli_reports_usage_and_exit_2_with_no_config_name() -> None:
    assert purity_linter.main([]) == 2
