"""Router zip-purity constraint test — LLD §7.1, I-8.

`make -C spine package-router` zips `spine/entrypoints/router.py` +
`spine/core/naming.py` (plus the package `__init__.py`s needed to import
them); §7.1 requires "an import test asserts the zip's transitive imports
stay within stdlib + boto3". This module:

1. Parses the Makefile's OWN `package-router:` recipe (frozen -- out of this
   bead's scope to edit) for its `cp <src> <dest>` lines, so the zip's file
   list here can never silently drift from what `make` actually ships --
   a change to that recipe fails this test loudly rather than leaving a
   stale hardcoded list.
2. Builds that exact zip into a **private `tmp_path`**, not the shared
   `spine/dist/` directory `make package-router` writes to -- other
   milestones' agents may run Makefile targets concurrently against the
   same `dist/`, so this test never touches it.
3. Runs `import spine.entrypoints.router` in a **subprocess** launched with
   `-S` (skips `site.py`, so `sys.path` starts as stdlib-only -- no
   site-packages, no pydantic, no pyspark reachable by accident) plus an
   explicit `sys.meta_path` import guard that raises on any import whose
   TOP-LEVEL module name is outside {stdlib} | {"spine", "boto3",
   "botocore", "jmespath", "s3transfer", "urllib3", "dateutil", "six"} --
   the "simplest robust approach" this bead's own scope note sanctions
   over curating individual site-packages directories. The guard is
   installed BEFORE the zip's `sys.path` entry is even added, so it is the
   FIRST thing consulted for every import router.py's chain triggers.
4. A companion test proves the guard/probe mechanism itself is not
   vacuously green: a synthetic zip with a poisoned `router.py` (`import
   pydantic`) MUST fail the same probe -- mutation-style self-check on the
   test's own bite.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import sysconfig
import zipfile
from pathlib import Path

_SPINE_ROOT = Path(__file__).resolve().parents[2]  # spine/ (uv-workspace module dir)
_MAKEFILE = _SPINE_ROOT / "Makefile"

# Only the packages `router.py`'s own import chain may legitimately reach,
# besides `spine` itself and the standard library: boto3 and boto3's own
# transitive runtime dependencies (Lambda's runtime already ships all of
# these together). Anything else reaching this point (pydantic, pyspark,
# pyiceberg, ...) is exactly what §7.1/I-8 forbids.
_ALLOWED_THIRD_PARTY_ROOTS = (
    "spine",
    "boto3",
    "botocore",
    "jmespath",
    "s3transfer",
    "urllib3",
    "dateutil",
    "six",
)

_GUARD_AND_IMPORT_TEMPLATE = """
import sys

sys.path[:0] = {paths!r}

_ALLOWED = frozenset(sys.stdlib_module_names) | set({allowed!r})


class _ImportGuard:
    def find_spec(self, fullname, path, target=None):
        root = fullname.split(".")[0]
        if root not in _ALLOWED:
            raise ImportError(
                "disallowed import outside stdlib + boto3 (LLD 004.1 s7.1/I-8): "
                + fullname
            )
        return None  # defer to the normal finders -- this guard only vetoes


sys.meta_path.insert(0, _ImportGuard())
import {module}  # noqa: F401
print("IMPORT_OK")
"""


def _parse_package_router_file_list() -> list[tuple[str, str]]:
    """Parses the Makefile's `package-router:` recipe for its `cp <src>
    <dest>` lines -> `[(src_relative_to_spine_root, arcname_under_dist_router),
    ...]`. Fails loudly (assertion) if the recipe's shape ever changes in a
    way this can't parse, rather than silently falling back to a stale list.
    """
    text = _MAKEFILE.read_text()
    match = re.search(r"^package-router:\n((?:\t.*\n?)+)", text, re.MULTILINE)
    assert match, "spine/Makefile's package-router: recipe not found -- has it been renamed?"
    recipe = match.group(1)

    cp_lines = re.findall(r"^\tcp (\S+) (\S+)$", recipe, re.MULTILINE)
    assert cp_lines, "package-router: recipe has no 'cp <src> <dest>' lines to parse"

    files = []
    for src, dest in cp_lines:
        assert dest.startswith("dist/router/"), f"unexpected cp destination shape: {dest!r}"
        files.append((src, dest.removeprefix("dist/router/")))
    return files


def _build_zip(
    dest_dir: Path,
    files: list[tuple[str, str]],
    overrides: dict[str, str] | None = None,
) -> Path:
    """Builds `router.zip` from `files` (`(src_relative_to_spine_root,
    arcname)` pairs, as parsed off the Makefile). `overrides` maps an
    arcname to literal source text instead of reading it off disk -- used
    by the poisoned-zip self-checks to substitute one file's content
    without ever touching the real one on disk."""
    overrides = overrides or {}
    zip_path = dest_dir / "router.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        for _src_rel, arcname in files:
            if arcname in overrides:
                zf.writestr(arcname, overrides[arcname])
            else:
                zf.write(_SPINE_ROOT / _src_rel, arcname)
    return zip_path


def _run_import_probe(zip_path: Path, tmp_path: Path, module: str = "spine.entrypoints.router"):
    site_packages = sysconfig.get_path("purelib")
    script = tmp_path / "probe.py"
    script.write_text(
        _GUARD_AND_IMPORT_TEMPLATE.format(
            paths=[str(zip_path), site_packages],
            allowed=list(_ALLOWED_THIRD_PARTY_ROOTS),
            module=module,
        )
    )
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    return subprocess.run(
        [sys.executable, "-S", str(script)],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(tmp_path),
        timeout=60,
    )


# --- the real §7.1 constraint test --------------------------------------------


def test_package_router_zip_file_list_matches_the_makefile_recipe() -> None:
    files = _parse_package_router_file_list()
    assert set(files) == {
        ("spine/__init__.py", "spine/__init__.py"),
        ("spine/entrypoints/__init__.py", "spine/entrypoints/__init__.py"),
        ("spine/entrypoints/router.py", "spine/entrypoints/router.py"),
        ("spine/core/__init__.py", "spine/core/__init__.py"),
        ("spine/core/naming.py", "spine/core/naming.py"),
    }


def test_router_zip_transitive_imports_stay_within_stdlib_and_boto3(tmp_path: Path) -> None:
    files = _parse_package_router_file_list()
    zip_path = _build_zip(tmp_path, files)

    result = _run_import_probe(zip_path, tmp_path)

    assert result.returncode == 0, (
        f"router zip's transitive imports left stdlib+boto3 "
        f"(LLD 004.1 s7.1/I-8):\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )
    assert "IMPORT_OK" in result.stdout


# --- self-check: the probe mechanism itself fails closed, not vacuously ------


def test_probe_fails_closed_on_a_poisoned_router(tmp_path: Path) -> None:
    """Proves `_run_import_probe` actually detects a purity regression,
    rather than passing regardless of `router.py`'s content: rebuilds the
    zip with a synthetic `router.py` that imports `pydantic`, and asserts
    the probe fails."""
    files = _parse_package_router_file_list()
    zip_path = _build_zip(
        tmp_path, files, overrides={"spine/entrypoints/router.py": "import pydantic\n"}
    )

    result = _run_import_probe(zip_path, tmp_path)

    assert result.returncode != 0
    assert "disallowed import outside stdlib + boto3" in result.stderr
    assert "pydantic" in result.stderr


def test_probe_fails_closed_on_a_poisoned_naming(tmp_path: Path) -> None:
    """Same self-check, but poisoning `core/naming.py` (the OTHER file the
    zip ships) rather than `router.py` itself -- guards against a future
    regression symmetric to the one this bead's own naming.py fix repaired
    (naming.py importing from `spine.core.model`, which is pydantic-shaped)."""
    files = _parse_package_router_file_list()
    zip_path = _build_zip(tmp_path, files, overrides={"spine/core/naming.py": "import pyspark\n"})

    result = _run_import_probe(zip_path, tmp_path, module="spine.core.naming")

    assert result.returncode != 0
    assert "disallowed import outside stdlib + boto3" in result.stderr
    assert "pyspark" in result.stderr
