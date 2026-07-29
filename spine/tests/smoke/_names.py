"""`SmokeNames` -- shared between `conftest.py` and `test_smoke.py`, a bare
top-level module (no `__init__.py` anywhere in `spine/tests/`, matching the
established convention: `tests/integration/killfx.py`/`snapshot_asserts.py`
are imported the same way, `import killfx`) rather than a package-qualified
import.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SmokeNames:
    """Every deployed resource name/identifier the smoke suite touches,
    derived from `name_prefix`/`env` per LLD 004.1 S5/S10 -- see
    `conftest.py`'s module docstring for why these are re-derived rather
    than read back via a shared config loader.
    """

    name_prefix: str
    env: str
    region: str

    @property
    def p(self) -> str:
        return f"{self.name_prefix}-{self.env}"

    @property
    def landing_bucket(self) -> str:
        return f"{self.p}-landing"

    @property
    def lake_bucket(self) -> str:
        return f"{self.p}-lake"

    @property
    def spine_db(self) -> str:
        return f"{self.name_prefix}_{self.env}_spine"

    @property
    def lake_db(self) -> str:
        # `modules/spine-pipeline`'s own "ambiguity 1" (main.tf header): the
        # lake db is never created by Terraform, only computed -- same
        # formula re-derived here.
        return f"{self.name_prefix}_{self.env}_lake"

    run_ledger_table: str = "run_ledger"

    @property
    def run_ledger_identifier(self) -> str:
        return f"{self.spine_db}.{self.run_ledger_table}"

    # The identity exemplar's table prefix. NOTE (bead handoff, flagged for
    # architect review, not resolved here): `modules/spine-pipeline`'s own
    # IAM grants (iam.tf) scope Glue/S3 access to `<slug>__*` /
    # `tables/<slug>/*` where `slug = slug(pipeline)` = "pipelines--identity"
    # (LLD S5's own formula) -- but the COMMITTED test-scope exemplar spec
    # (`tests/exemplar/identity/pipeline.yaml`) instead names its tables
    # "identity__raw" etc (no "pipelines--" prefix), which the deployed job
    # role could not actually read/write under S5's grant scoping. This
    # suite assumes whichever `pipeline.yaml` gets pushed to
    # `s3://<p>-artifacts/spine/specs/identity/pipeline.yaml` for the REAL
    # deploy uses the IAM-consistent (slug-prefixed) table names -- if the
    # deploy instead pushes the committed exemplar file verbatim, this
    # suite's facts/state polling will time out, which is itself a useful
    # signal that the discrepancy needs resolving before M6 closes.
    identity_table_prefix: str = "pipelines--identity"

    @property
    def identity_facts_table(self) -> str:
        return f"{self.lake_db}.{self.identity_table_prefix}__facts"

    @property
    def identity_state_table(self) -> str:
        return f"{self.lake_db}.{self.identity_table_prefix}__state"

    @property
    def glue_job_log_group(self) -> str:
        # `modules/spine-pipeline/glue.tf`: "/aws-glue/spine/${p}-spine-<slug>"
        return f"/aws-glue/spine/{self.p}-spine-{self.identity_table_prefix}"
