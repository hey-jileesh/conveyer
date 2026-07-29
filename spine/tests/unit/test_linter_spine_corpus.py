"""Unit tests for the promoted `tools/purity_linter.py` engine, exercised
against spine's own config (LLD 004.1 I-15/§12.3; R-11, bead conveyer-nvh.28,
M5).

Mirrors `ingestion/tests/unit/test_purity_linter.py`'s pattern exactly: `tools/`
(repo root) is not itself a package, so it is imported here via an explicit
`sys.path` entry rather than a package-relative import; every fixture's
source text is fed to `lint_source` alongside a *simulated* `rel_path` that
stands in for "where this file would live in the real tree" -- that rel_path
is what selects which of spine's three `ScopeProfile`s (`core`,
`frames-transforms`, `effects-stages`) apply.

Fixtures live in `tests/unit/linter_fixtures/` (this file's sibling), each
named `pass_*.py`/`fail_*.py`, per §12.3's named corpus list. They are never
placed inside the real `spine/spine/**`/`spine/pipelines/**` tree -- doing so
would make `make -C spine lint` fail on the deliberately-bad fixtures
themselves (same reasoning as ingestion's `purity_fixtures/`); `tests/unit/`
itself is outside spine's `walk_roots` (`("spine", "pipelines")`, both
relative to `package_root="spine"`), so this placement is a genuine no-op
from the real linter's own point of view, confirmed below by
`test_discover_files_only_walks_spine_and_pipelines`.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_TOOLS_DIR = Path(__file__).resolve().parents[3] / "tools"
if str(_TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(_TOOLS_DIR))

import purity_linter  # noqa: E402  (path setup must precede this import)
from linter_configs import spine as spine_config  # noqa: E402

_CONFIG = spine_config.CONFIG
_FIXTURES_DIR = Path(__file__).resolve().parent / "linter_fixtures"


def _fixture_source(name: str) -> str:
    return (_FIXTURES_DIR / name).read_text(encoding="utf-8")


# (fixture filename, simulated rel_path, expected rule substrings — every
# entry must appear as a substring of at least one reported "rule" field).
# The five §12.3-named fixtures, one per bullet of the spine config's rule
# groups.
MUST_FAIL_CASES: list[tuple[str, str, tuple[str, ...]]] = [
    (
        "fail_transforms_spark_read.py",
        "spine/frames/x.py",
        # Renamed from "purity-banned-call" to "purity-banned-attr" (bead
        # conveyer-nvh.42, F3(a)): `banned_attr_names` now fires on ANY
        # `ast.Attribute` node by name, not only one sitting in call
        # position, so the rule name no longer implies "call".
        ("purity-banned-attr:read",),
    ),
    (
        "fail_transforms_collect.py",
        "pipelines/identity/transforms.py",
        ("purity-banned-attr:collect",),
    ),
    (
        "fail_core_pyspark_import.py",
        "spine/core/x.py",
        ("purity-banned-import:pyspark",),
    ),
    (
        "fail_effects_fstring_where.py",
        "spine/effects/x.py",
        ("idiom-string-sql:where",),
    ),
    # Design-critique finding F3 (bead conveyer-nvh.42): the two gaps below.
    (
        "fail_transforms_sparksession_chain.py",
        "spine/frames/x.py",
        ("purity-banned-attr:sparkSession", "purity-banned-attr:read"),
    ),
    (
        "fail_effects_awsglue_import.py",
        "spine/effects/x.py",
        ("purity-banned-import:awsglue",),
    ),
]

MUST_PASS_CASES: list[tuple[str, str]] = [
    ("pass_frames_functions_only.py", "spine/frames/x.py"),
    # the frames-transforms profile also covers pipeline transforms.py and
    # (per §12.3's own note) tests/exemplar -- same clean fixture, different
    # simulated location, confirming the profile match is path-driven, not
    # filename-driven.
    ("pass_frames_functions_only.py", "pipelines/identity/transforms.py"),
]


@pytest.mark.parametrize("fixture_name,rel_path,expected_rules", MUST_FAIL_CASES)
def test_must_fail_fixture_reports_violation(
    fixture_name: str, rel_path: str, expected_rules: tuple[str, ...]
) -> None:
    violations = purity_linter.lint_source(_fixture_source(fixture_name), rel_path, _CONFIG)

    assert violations, f"expected at least one violation for {fixture_name}"
    reported = [f"{v.rel_path}:{v.line}:{v.rule}" for v in violations]
    for expected in expected_rules:
        assert any(expected in r for r in reported), (
            f"{fixture_name}: expected a rule containing {expected!r}, got {reported}"
        )
    for v in violations:
        assert v.rel_path == rel_path
        assert v.line > 0
        assert v.rule


@pytest.mark.parametrize("fixture_name,rel_path", MUST_PASS_CASES)
def test_must_pass_fixture_is_clean(fixture_name: str, rel_path: str) -> None:
    violations = purity_linter.lint_source(_fixture_source(fixture_name), rel_path, _CONFIG)
    assert violations == (), f"{fixture_name}: expected no violations, got {violations}"


def test_effects_stages_string_sql_exemption_targets_a_real_function() -> None:
    """§12.3: the rendered MERGE in `effects/spark.py` is the single
    hardcoded string-SQL exemption -- verify the `(file, function)` pair
    named in `_STRING_SQL_EXEMPTION` still matches a real function in the
    real file (not a stale name after a rename), and that the real file
    reports zero `idiom-string-sql` violations either way. `render_merge`'s
    own docstring records why the exemption is currently a documented no-op
    (`spark.sql(sql_text)` is called with a plain variable, never an inline
    f-string) -- this test pins that finding as a regression guard rather
    than re-deriving it."""
    (rel_path, function_name) = next(iter(_CONFIG.string_sql_exemption))
    # `rel_path` is relative to the engine's own `root` (`config.package_root`
    # below the repo root, i.e. the `spine` uv-workspace module dir) -- see
    # `LinterConfig.package_root`'s docstring; `parents[2]` of THIS file
    # (`spine/tests/unit/test_linter_spine_corpus.py`) is exactly that dir.
    root = Path(__file__).resolve().parents[2]
    path = root / rel_path
    assert path.is_file(), f"string_sql_exemption names a missing file: {rel_path}"

    import ast

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=rel_path)
    function_names = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    assert function_name in function_names, (
        f"string_sql_exemption names a missing function: {rel_path}::{function_name}"
    )

    violations = purity_linter.lint_file(path, root, _CONFIG)
    string_sql = [v for v in violations if v.rule.startswith("idiom-string-sql")]
    assert string_sql == [], f"unexpected string-SQL violations in {rel_path}: {string_sql}"


@pytest.mark.parametrize(
    "rel_path",
    [
        "spine/effects/spark.py",
        "spine/stages/apply.py",
        "spine/entrypoints/glue_main.py",
        "spine/bootstrap/create_run_ledger.py",
        "spine/run.py",
        "spine/binding.py",
        "spine/config.py",
        "spine/context.py",
        "pipelines/identity/transforms.py",
    ],
)
def test_package_wide_profile_bans_awsglue_everywhere(rel_path: str) -> None:
    """Design-critique finding F3(b) (bead conveyer-nvh.42): before the
    package-wide profile existed, `effects/`, `stages/`, `entrypoints/`,
    `bootstrap/`, and spine's top-level modules matched NONE of the `core`
    / `frames-transforms` profiles' `path_prefixes`, so `import awsglue`
    there tripped no rule at all. Every one of these paths now reports the
    package-wide `awsglue` ban directly (not routed through some other
    profile's own duplicated entry)."""
    profiles = purity_linter._matching_profiles(rel_path, _CONFIG)
    package_wide = next(p for p in profiles if p.name == "package-wide")
    assert "awsglue" in package_wide.banned_import_roots

    violations = purity_linter.lint_source("import awsglue\n", rel_path, _CONFIG)
    assert any(v.rule == "purity-banned-import:awsglue" for v in violations), (
        f"{rel_path}: expected purity-banned-import:awsglue, got {violations}"
    )


def test_discover_files_only_walks_spine_and_pipelines(tmp_path: Path) -> None:
    (tmp_path / "spine" / "core").mkdir(parents=True)
    (tmp_path / "spine" / "core" / "a.py").write_text("x = 1\n", encoding="utf-8")
    (tmp_path / "pipelines" / "identity").mkdir(parents=True)
    (tmp_path / "pipelines" / "identity" / "transforms.py").write_text("y = 2\n", encoding="utf-8")
    (tmp_path / "tests" / "unit" / "linter_fixtures").mkdir(parents=True)
    (tmp_path / "tests" / "unit" / "linter_fixtures" / "not_walked.py").write_text(
        "import pyspark\n", encoding="utf-8"
    )

    found = purity_linter._discover_files(tmp_path, _CONFIG)
    rels = sorted(p.relative_to(tmp_path).as_posix() for p in found)

    assert rels == ["pipelines/identity/transforms.py", "spine/core/a.py"]


def test_real_spine_and_pipelines_tree_is_purity_clean() -> None:
    """Regression guard tied to the R-11 acceptance gate: whatever
    `main()`/`make -C spine lint` would report against the real
    `spine/spine/**` + `spine/pipelines/**` tree is zero violations."""
    root = Path(__file__).resolve().parents[2]  # .../conveyer/spine
    files = purity_linter._discover_files(root, _CONFIG)
    assert files, "expected to discover at least one real source file"

    all_violations = [v for f in files for v in purity_linter.lint_file(f, root, _CONFIG)]
    for f in files:
        rel = f.relative_to(root).as_posix()
        assert rel.startswith("spine/") or rel.startswith("pipelines/")
    assert all_violations == [], f"unexpected real-tree violations: {all_violations}"


def test_main_returns_zero_against_the_real_spine_root(capsys: pytest.CaptureFixture[str]) -> None:
    root = Path(__file__).resolve().parents[2]  # .../conveyer/spine
    exit_code = purity_linter.main(config=_CONFIG, root=root)
    out = capsys.readouterr().out

    assert exit_code == 0
    assert out == ""
