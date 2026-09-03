"""`apply` for the identity exemplar's violations variant. LLD 006.1 §12.2/G-12, R-04.

`apply` is `pipelines.identity.transforms.apply`, re-exported (not
re-implemented — a single source of truth for the column projection and the
one-entry candidate mapping, 006.1 §4.4).

**006.1 migration (bead conveyer-6pg.13, B3): `post_check` is GONE.** The
005.1-era `post_check` flagged every candidate row whose `payload` carried
the fixture's own violation marker (`_VIOLATION_MARKER = "INVALID"`) under
the free-text-then-A-14-grammar-checked reason `business/negative-amount`.
Under the framework's own interpreter (§7) that rule is now DECLARED DATA —
a `row` check in `checks.yaml` (G-12: "violations variant's rules live in
checks.yaml (`business/negative-amount`)"), bind-time validated (K1-K9,
006.1 §5.4) rather than pipeline-Python-authored and runtime-grammar-
checked (A-14's runtime check is superseded, §5.4 K6). This module
therefore contributes zero check code — `_VIOLATION_MARKER` stays here only
as the one shared literal the fixture's own CSV rows and the checks
declaration must agree on (fixtures live in `tests/exemplar/identity/
fixtures/violations/*.csv`; the checks declaration lives with each test's
own `PipelineSpecModel` construction — `tests/integration/
scenario_helpers.py::VIOLATIONS_CHECKS`).
"""

from __future__ import annotations

from pipelines.identity.transforms import FACT_TYPE, apply  # noqa: F401 -- re-exported

# The fixture's own marker for a row the `business/negative-amount` checks.yaml
# rule must flag -- see tests/exemplar/identity/fixtures/violations/*.csv and
# tests/integration/scenario_helpers.py::VIOLATIONS_CHECKS's authored `expr`.
VIOLATION_MARKER = "INVALID"
# 006.1 §5.4 K6/A-14: the governed business reason code this fixture's
# violation quarantines under -- now declared in checks.yaml, not minted here.
VIOLATION_REASON = "business/negative-amount"
