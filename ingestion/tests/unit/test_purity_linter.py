"""Unit tests for the promoted `tools/purity_linter.py` engine, exercised
against ingestion's own config (LLD 004.1 I-15/§12.3; pre-promotion LLD
002.1 §12.2).

`tools/` (repo root) is not itself a package (it lives outside `ingestion/`,
deliberately not part of any config's walked tree — see the module docstring
in purity_linter.py), so it is imported here via an explicit `sys.path`
entry rather than a package-relative import. `linter_configs.ingestion`
supplies the `LinterConfig` that used to be hardcoded module-level constants
in this same file's linter — every call below threads it through explicitly
(the engine takes `config` as a real parameter now); this is the one
mechanical adjustment the promotion required. No assertion or fixture below
changed.

Fixtures live in `tests/unit/purity_fixtures/`, each named `pass_*.py` or
`fail_*.py`. They are never placed inside the real `ingestion/**`/`sources/**`
tree: doing so would make `make lint` fail on the deliberately-bad fixtures
themselves. Instead each fixture's source text is fed to `lint_source`
alongside a *simulated* `rel_path` that stands in for "where this file would
live in the real tree" — that rel_path is what selects which rule group(s)
apply and is what a real `make lint` run would report.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_TOOLS_DIR = Path(__file__).resolve().parents[3] / "tools"
if str(_TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(_TOOLS_DIR))

import purity_linter  # noqa: E402  (path setup must precede this import)
from linter_configs import ingestion as ingestion_config  # noqa: E402

_CONFIG = ingestion_config.CONFIG
_FIXTURES_DIR = Path(__file__).resolve().parent / "purity_fixtures"


def _fixture_source(name: str) -> str:
    return (_FIXTURES_DIR / name).read_text(encoding="utf-8")


# (fixture filename, simulated rel_path, expected rule substrings — every
# entry must appear as a substring of at least one reported "rule" field)
MUST_FAIL_CASES: list[tuple[str, str, tuple[str, ...]]] = [
    ("fail_banned_submodule_from_import.py", "ingestion/core/x.py", ("purity-banned-import:os",)),
    ("fail_aliased_import.py", "ingestion/core/x.py", ("purity-banned-import:boto3",)),
    ("fail_nested_class.py", "ingestion/effects/x.py", ("idiom-class",)),
    (
        "fail_unittest_mock_import.py",
        "ingestion/effects/x.py",
        ("idiom-banned-import:unittest.mock",),
    ),
    ("fail_try_in_core.py", "ingestion/core/other.py", ("purity-try",)),
    ("fail_raise_in_core.py", "ingestion/core/other.py", ("purity-raise",)),
    (
        "fail_banned_call_datetime_now.py",
        "ingestion/core/x.py",
        ("purity-banned-call:datetime.now",),
    ),
    ("fail_banned_call_uuid4.py", "ingestion/core/x.py", ("purity-banned-call:uuid.uuid4",)),
    ("fail_class_random_base.py", "ingestion/effects/x.py", ("idiom-class",)),
    # parse_manifest try/except allowlist is keyed to the EXACT (file,
    # function) pair — each of these misses on one half of that pair and
    # must therefore still be flagged.
    (
        "fail_try_wrong_function_in_completeness.py",
        "ingestion/core/completeness.py",
        ("purity-try",),
    ),
    (
        "fail_try_parse_manifest_wrong_file.py",
        "ingestion/core/other_module.py",
        ("purity-try",),
    ),
    # conveyer-4ot.24: `try` inside a validator-decorated method is NOT
    # covered by the validator-raise exemption -- it stays banned.
    ("fail_try_inside_validator.py", "ingestion/core/x.py", ("purity-try",)),
    # conveyer-4ot.24: the second (file, function) allowlist entry
    # (model.py::_check_iana_timezone) is keyed exactly, same as the first.
    (
        "fail_check_iana_timezone_wrong_function.py",
        "ingestion/core/model.py",
        ("purity-try", "purity-raise"),
    ),
    (
        "fail_check_iana_timezone_wrong_file.py",
        "ingestion/core/other_module.py",
        ("purity-try", "purity-raise"),
    ),
    # conveyer-4ot.26: the third hardcoded (file, class name) allowlist
    # entry (TransientError) is keyed exactly, same as the try/raise pairs.
    (
        "fail_transient_error_wrong_file.py",
        "ingestion/effects/other.py",
        ("idiom-class",),
    ),
    # conveyer-4ot.26: closed enumeration, not a general Exception-subclass
    # exemption -- a different class name in the SAME allowlisted file must
    # still be flagged.
    (
        "fail_other_exception_subclass_in_effects.py",
        "ingestion/effects/records.py",
        ("idiom-class",),
    ),
]

MUST_PASS_CASES: list[tuple[str, str]] = [
    ("pass_dataclass_decorator_call_style.py", "ingestion/core/x.py"),
    ("pass_dataclasses_dataclass_frozen.py", "ingestion/core/x.py"),
    ("pass_pydantic_basemodel_with_validator.py", "ingestion/core/x.py"),
    ("pass_enum_class.py", "ingestion/core/x.py"),
    ("pass_parse_manifest_allowlist.py", "ingestion/core/completeness.py"),
    ("pass_effects_module_with_boto3.py", "ingestion/effects/s3.py"),
    ("pass_clean_core_module.py", "ingestion/core/x.py"),
    # conveyer-4ot.24: raise inside @field_validator/@model_validator
    # methods is exempt from purity-raise (bare/attribute/call decorator
    # forms, @classmethod-paired or not).
    ("pass_validator_raise.py", "ingestion/core/x.py"),
    # conveyer-4ot.24: the second hardcoded (file, function) allowlist
    # entry -- both try and raise exempt when file+function match exactly.
    ("pass_check_iana_timezone_allowlist.py", "ingestion/core/model.py"),
    # conveyer-4ot.26: the third hardcoded (file, class name) allowlist
    # entry -- TransientError(Exception) exempt from idiom-class when file+
    # class name match exactly.
    ("pass_transient_error_allowlist.py", "ingestion/effects/records.py"),
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
    # every reported violation is a well-formed file:line:rule triple against
    # the simulated location
    for v in violations:
        assert v.rel_path == rel_path
        assert v.line > 0
        assert v.rule


@pytest.mark.parametrize("fixture_name,rel_path", MUST_PASS_CASES)
def test_must_pass_fixture_is_clean(fixture_name: str, rel_path: str) -> None:
    violations = purity_linter.lint_source(_fixture_source(fixture_name), rel_path, _CONFIG)
    assert violations == (), f"{fixture_name}: expected no violations, got {violations}"


def test_validator_raise_exemption_covers_bare_and_attribute_decorator_forms() -> None:
    """conveyer-4ot.24: the decorator match is by terminal name, so
    `@field_validator`/`@pydantic.model_validator` (no call at all) are
    exempt too, not just the call form used throughout the fixtures."""
    source = (
        "from pydantic import field_validator\n"
        "import pydantic\n\n"
        "class Foo:\n"
        "    @field_validator\n"
        "    def bare_form(cls, value):\n"
        "        raise ValueError('bare')\n\n"
        "    @pydantic.model_validator\n"
        "    def attr_bare_form(self):\n"
        "        raise ValueError('attr bare')\n"
    )
    violations = purity_linter.lint_source(source, "ingestion/core/x.py", _CONFIG)
    assert not any(v.rule == "purity-raise" for v in violations)


def test_undecorated_method_raise_is_not_exempt() -> None:
    """A plain method with no field_validator/model_validator decorator gets
    no exemption, even sitting alongside decorated methods in the same
    class."""
    source = (
        "from pydantic import BaseModel, field_validator\n\n"
        "class Foo(BaseModel):\n"
        "    value: str\n\n"
        "    @field_validator('value')\n"
        "    @classmethod\n"
        "    def _check_value(cls, value):\n"
        "        if not value:\n"
        "            raise ValueError('empty')\n"
        "        return value\n\n"
        "    def _not_a_validator(self):\n"
        "        raise ValueError('should NOT be exempt')\n"
    )
    violations = purity_linter.lint_source(source, "ingestion/core/x.py", _CONFIG)
    raise_lines = [v.line for v in violations if v.rule == "purity-raise"]
    assert raise_lines == [14]  # only the undecorated method's raise


def test_try_inside_validator_is_flagged_but_its_raise_stays_exempt() -> None:
    """`fail_try_inside_validator.py` exercises the exact boundary of the
    conveyer-4ot.24 exemption: `try` is banned inside a validator body, but
    the `raise` in that same body's `except` clause stays exempt."""
    source = _fixture_source("fail_try_inside_validator.py")
    violations = purity_linter.lint_source(source, "ingestion/core/x.py", _CONFIG)
    assert [v.rule for v in violations] == ["purity-try"]


def test_real_model_and_completeness_modules_are_purity_clean() -> None:
    """Regression guard for the LLD §6.1/§12.2 contradiction resolved by
    conveyer-4ot.24: the real `core/model.py` (validator raises) and
    `core/completeness.py` (`parse_manifest`'s try/except) must report zero
    purity-try/purity-raise violations once loaded through the real repo
    tree walk (not just the simulated fixtures above)."""
    root = Path(__file__).resolve().parents[2]
    for rel in ("ingestion/core/model.py", "ingestion/core/completeness.py"):
        path = root / rel
        assert path.is_file(), f"expected {rel} to exist in the real tree"
        violations = purity_linter.lint_file(path, root, _CONFIG)
        control_flow = [v for v in violations if v.rule in ("purity-try", "purity-raise")]
        assert control_flow == [], f"{rel}: unexpected control-flow violations {control_flow}"


def test_real_records_module_transient_error_is_idiom_clean() -> None:
    """Regression guard for the LLD §7.3/§12.2 contradiction resolved by
    conveyer-4ot.26: the real `effects/records.py`'s `class
    TransientError(Exception)` must report zero `idiom-class` violations once
    loaded through the real repo tree walk (this is the exact violation that
    blocked `make lint` per conveyer-4ot.14's findings)."""
    root = Path(__file__).resolve().parents[2]
    rel = "ingestion/effects/records.py"
    path = root / rel
    assert path.is_file(), f"expected {rel} to exist in the real tree"
    violations = purity_linter.lint_file(path, root, _CONFIG)
    idiom_class = [v for v in violations if v.rule == "idiom-class"]
    assert idiom_class == [], f"{rel}: unexpected idiom-class violations {idiom_class}"


def test_uuid5_is_not_banned() -> None:
    """uuid.uuid5 is deterministic given its inputs — the LLD explicitly
    excludes it from the banned-call list ("now" is always a parameter)."""
    source = "import uuid\n\ndef new_id(ns, name):\n    return uuid.uuid5(ns, name)\n"
    violations = purity_linter.lint_source(source, "ingestion/core/ids.py", _CONFIG)
    assert violations == ()


def test_effects_and_entrypoints_are_exempt_from_purity_but_not_idiom() -> None:
    source = (
        "import boto3\n"
        "from datetime import datetime\n\n"
        "def make_fx(client):\n"
        "    def now():\n"
        "        return datetime.now()\n"
        "    return now\n"
    )
    # effects/: purity rules do not apply
    assert purity_linter.lint_source(source, "ingestion/effects/s3.py", _CONFIG) == ()

    # a banned-everywhere idiom import still applies to effects/
    mock_source = "from unittest import mock\n"
    violations = purity_linter.lint_source(mock_source, "ingestion/effects/s3.py", _CONFIG)
    assert any(v.rule == "idiom-banned-import:unittest.mock" for v in violations)


def test_sources_tree_is_full_purity_scope() -> None:
    source = "import requests\n"
    violations = purity_linter.lint_source(source, "sources/carrier-x/pull.py", _CONFIG)
    assert any(v.rule == "purity-banned-import:requests" for v in violations)


def test_discover_files_only_walks_ingestion_and_sources(tmp_path: Path) -> None:
    (tmp_path / "ingestion" / "core").mkdir(parents=True)
    (tmp_path / "ingestion" / "core" / "a.py").write_text("x = 1\n", encoding="utf-8")
    (tmp_path / "sources" / "plug").mkdir(parents=True)
    (tmp_path / "sources" / "plug" / "b.py").write_text("y = 2\n", encoding="utf-8")
    (tmp_path / "tools").mkdir(parents=True)
    (tmp_path / "tools" / "not_walked.py").write_text("import os\n", encoding="utf-8")
    (tmp_path / "tests").mkdir(parents=True)
    (tmp_path / "tests" / "not_walked_either.py").write_text("import os\n", encoding="utf-8")

    found = purity_linter._discover_files(tmp_path, _CONFIG)
    rels = sorted(p.relative_to(tmp_path).as_posix() for p in found)

    assert rels == ["ingestion/core/a.py", "sources/plug/b.py"]


def test_lint_file_reports_rel_path_and_finds_real_violation(tmp_path: Path) -> None:
    core_dir = tmp_path / "ingestion" / "core"
    core_dir.mkdir(parents=True)
    bad_file = core_dir / "bad.py"
    bad_file.write_text("import os\n", encoding="utf-8")

    violations = purity_linter.lint_file(bad_file, tmp_path, _CONFIG)

    assert violations == (
        purity_linter.Violation("ingestion/core/bad.py", 1, "purity-banned-import:os"),
    )


def test_main_returns_zero_and_prints_nothing_when_tree_is_clean(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    core_dir = tmp_path / "ingestion" / "core"
    core_dir.mkdir(parents=True)
    (core_dir / "good.py").write_text(
        "from dataclasses import dataclass\n\n@dataclass(frozen=True)\nclass Foo:\n    x: int\n",
        encoding="utf-8",
    )

    # `root=` supplied directly: post-promotion, `main()`'s own `__file__` no
    # longer sits one hop above the walked tree (the engine now lives at the
    # shared repo-root `tools/`, see `LinterConfig.package_root`'s docstring),
    # so a synthetic tree is exercised by passing `root` rather than by
    # monkeypatching `purity_linter.__file__`.
    exit_code = purity_linter.main(config=_CONFIG, root=tmp_path)
    out = capsys.readouterr().out

    assert exit_code == 0
    assert out == ""


def test_main_returns_nonzero_and_prints_violations_when_tree_is_dirty(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    core_dir = tmp_path / "ingestion" / "core"
    core_dir.mkdir(parents=True)
    (core_dir / "bad.py").write_text("import os\n", encoding="utf-8")

    exit_code = purity_linter.main(config=_CONFIG, root=tmp_path)
    out = capsys.readouterr().out

    assert exit_code == 1
    assert out.strip() == "ingestion/core/bad.py:1:purity-banned-import:os"


def test_real_repo_scaffold_walk_matches_current_source_tree() -> None:
    """Regression guard tied to the acceptance gate: whatever `main()` would
    report against the real module tree is exactly what `lint_file` reports
    file-by-file — i.e. the CLI wiring does not silently drop or duplicate
    violations relative to the pure `lint_source` core exercised above."""
    root = Path(__file__).resolve().parents[2]
    files = purity_linter._discover_files(root, _CONFIG)
    assert files, "expected to discover at least one real source file"

    all_violations = [v for f in files for v in purity_linter.lint_file(f, root, _CONFIG)]
    # every discovered file is genuinely inside ingestion/ or sources/
    for f in files:
        rel = f.relative_to(root).as_posix()
        assert rel.startswith("ingestion/") or rel.startswith("sources/")
    # every violation's rel_path corresponds to one of the discovered files
    discovered_rels = {f.relative_to(root).as_posix() for f in files}
    for v in all_violations:
        assert v.rel_path in discovered_rels
