"""Drift guard: the deployed Glue job's `default_arguments` (`spine/
terraform/modules/spine-pipeline/glue.tf`) must set every REQUIRED
`--conveyer-*` argv key `spine.config.RunnerConfig.from_args` needs --
LLD S6.4/S10.4. Originating gap (`conveyer-swb.19`): a new required
`RunnerConfig` field (`artifacts_bucket`) was added without updating the
deployed job's default arguments, which would have made the NEXT Glue job
start raise `KeyError` at config parse. This bead (`conveyer-swb.20`) fixes
that instance and adds this test so the class of drift fails CI going
forward.

Pure file-read/text-parse -- no Terraform binary, no AWS, no `spine`
Spark deps beyond importing `spine.config` itself (a plain dataclass +
pydantic module). Fast, CI-cheap companion to `spine/terraform/modules/
spine-pipeline/tests/spine_pipeline.tftest.hcl`'s own `terraform test`
assertion of the same fact -- that one exercises the module's actual
planned resource; this one never invokes `terraform` and needs nothing on
PATH.

**Required-ness is derived, not hand-copied**, from `RunnerConfig`'s own
type hints: a field typed `X | None` is optional (`warehouse_uri`,
`ledger_sql_uri` -- both test-only per `config.py`'s own `from_args`/
`optional()` calls); every other field mapped in `_ARGV_KEYS` is required.
Deriving from the dataclass instead of hardcoding a key list means a
FUTURE new required field automatically joins this test's expectation with
no edit here needed -- the same class of drift `conveyer-swb.19` caused.

**Deliberately excluded** from the "glue.tf must set this" expectation
(mirrors glue.tf's own header comment, and the bead brief):
  - `--conveyer-warehouse-uri` / `--conveyer-ledger-sql-uri`: optional
    `RunnerConfig` fields (test-only; this job runs `catalog_kind="glue"` /
    `ledger_catalog_kind="glue"`, so `from_args` never reaches the branch
    that needs them) -- already excluded by the optional-field derivation
    above; not part of `_SFN_INJECTED_KEYS` below.
  - `--conveyer-delivery` / `--conveyer-sfn-retry-count` /
    `--conveyer-sfn-redrive-count`: SFN-injected PER EXECUTION via the
    state machine's Task `Arguments` (`state_machine.tf`) -- a
    `default_arguments` constant would be wrong for a per-batch/per-retry
    value, so these are excluded explicitly via `_SFN_INJECTED_KEYS`.
  - `--conveyer-attempt-id` is not even a member of `_ARGV_KEYS` (I-5's
    `--conveyer-attempt-id` -> `--JOB_RUN_ID` fallback is hardcoded in
    `from_args`, not table-driven), so it never enters the derived
    required set in the first place -- nothing to exclude.
"""

from __future__ import annotations

import dataclasses
import re
import typing
from pathlib import Path

import pytest
from spine import config

_REPO_ROOT = Path(__file__).resolve().parents[3]  # .../conveyer
_GLUE_TF_PATH = _REPO_ROOT / "spine" / "terraform" / "modules" / "spine-pipeline" / "glue.tf"

# SFN-injected per-execution keys (state_machine.tf's Task `Arguments`) --
# a `default_arguments` constant would be wrong for these, by design.
_SFN_INJECTED_KEYS: frozenset[str] = frozenset(
    {
        "--conveyer-delivery",
        "--conveyer-sfn-retry-count",
        "--conveyer-sfn-redrive-count",
    }
)

_CONVEYER_KEY_RE = re.compile(r'"--(conveyer-[a-z0-9-]+)"\s*=')


def _extract_default_arguments_block(hcl_text: str) -> str:
    """Textual (non-HCL-parsing) extraction of the `default_arguments = {
    ... }` body from a `glue.tf`-shaped Terraform file: finds the block's
    opening brace after the `default_arguments` marker, then returns
    everything up to the MATCHING close brace via depth counting. Exact
    (not a heuristic) for this file's shape, since every value inside the
    block is a single-line scalar/reference -- no nested `{`/`}` appears.
    Raises `ValueError` if the marker or a matching close brace is not
    found, so a future file-shape drift fails loudly rather than silently
    returning an empty/wrong slice.
    """
    marker = "default_arguments"
    marker_idx = hcl_text.find(marker)
    if marker_idx == -1:
        raise ValueError(f"{marker!r} not found in HCL text")
    open_idx = hcl_text.find("{", marker_idx)
    if open_idx == -1:
        raise ValueError(f"no '{{' found after {marker!r}")
    depth = 0
    for i in range(open_idx, len(hcl_text)):
        ch = hcl_text[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return hcl_text[open_idx + 1 : i]
    raise ValueError(f"no matching '}}' found for {marker!r} block")


def _conveyer_argv_keys(block_text: str) -> frozenset[str]:
    """Every literal `"--conveyer-<kebab>"` key assigned inside a
    `default_arguments` block body, as `--conveyer-<kebab>` strings
    (matching `spine.config._ARGV_KEYS`' value shape, sans the injected
    `--`)."""
    return frozenset("--" + m.group(1) for m in _CONVEYER_KEY_RE.finditer(block_text))


def _required_argv_keys() -> frozenset[str]:
    """Every `--conveyer-*` argv key `RunnerConfig.from_args` treats as
    REQUIRED, derived from `RunnerConfig`'s own field types (see module
    docstring) rather than hand-copied."""
    hints = typing.get_type_hints(config.RunnerConfig)
    out: set[str] = set()
    for field in dataclasses.fields(config.RunnerConfig):
        field_type = hints[field.name]
        if type(None) in typing.get_args(field_type):
            continue  # optional field (warehouse_uri, ledger_sql_uri) -- test-only
        key = config._ARGV_KEYS.get(field.name)
        if key is not None:
            out.add("--" + key)
    return frozenset(out)


def test_glue_tf_default_arguments_sets_every_required_conveyer_key() -> None:
    hcl_text = _GLUE_TF_PATH.read_text()
    block = _extract_default_arguments_block(hcl_text)
    found = _conveyer_argv_keys(block)

    expected = _required_argv_keys() - _SFN_INJECTED_KEYS
    missing = expected - found
    assert not missing, (
        f"{_GLUE_TF_PATH} default_arguments is missing required --conveyer-* "
        f"keys that spine.config.RunnerConfig.from_args needs at Glue job "
        f"start: {sorted(missing)}"
    )


def test_extract_default_arguments_block_raises_without_marker() -> None:
    with pytest.raises(ValueError, match="'default_arguments' not found"):
        _extract_default_arguments_block("no marker here")


def test_extract_default_arguments_block_raises_on_unterminated_block() -> None:
    with pytest.raises(ValueError, match="no matching '}' found"):
        _extract_default_arguments_block("default_arguments = { unterminated")
