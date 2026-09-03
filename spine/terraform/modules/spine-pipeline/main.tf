# modules/spine-pipeline -- LLD 004.1 S10.4 + the `spine-job-<slug>` row of
# S10.3 (I-21) + the S8.2 state-machine template. One instance per pipeline
# (`for_each`'d once per pipeline by the env root, mirroring
# `ingestion/terraform/modules/feed`'s own per-feed instantiation pattern --
# S10.7's reasoning restated here: the resources below are per-pipeline, so
# this module must be too).
#
# Builds: the Glue job (`${p}-spine-<slug>`), its own IAM role
# (`spine-job-<slug>`, I-21 -- NOT the shared platform `spine-sfn` role),
# the per-pipeline Standard state machine (S8.2 template, verbatim), its
# explicit CloudWatch log group, and the four per-pipeline alarm rows from
# S11.4 (the platform-scoped alarm rows live in the sibling
# `modules/spine-platform`, per that module's own monitoring.tf header).
#
# Consumes TWO upstream modules as flat variables, never as
# `terraform_remote_state` (env-root composition, decided at M6 per S10.1):
#   - the ingestion platform's shared bus + landing/lake/artifacts buckets
#     (`module.platform.*` outputs -- these are the SAME physical resources
#     ingestion already uses; spine does not duplicate them);
#   - the sibling `modules/spine-platform`'s own outputs (`spine_database_
#     name` == its `glue_database_name` output, `spine_sfn_role_arn`,
#     `run_ledger_table_name`) -- consumed as variables, never referenced by
#     module path, so this module validates standalone (task instruction:
#     do not touch `envs/dev`, a later bead wires the env root).
#
# Like `ingestion/terraform/modules/feed` and `modules/platform`, this module
# deliberately does NOT declare a `provider "aws" {}` block: it is always
# `for_each`'d at the call site, and Terraform forbids a module used with
# `for_each`/`count`/`depends_on` from configuring its own provider.
#
# --- Genuine LLD ambiguities resolved here (see handoff report for the
# full list) -----------------------------------------------------------
#
#   1. The LAKE Glue database (`${name_prefix}_${env}_lake`, LLD S5 --
#      holds `<slug>__raw/quarantine/facts/state`) is never created by any
#      Terraform module in this LLD's scope: S10.2 lists only the SPINE db
#      for `modules/spine-platform`; S10.5 says the exemplar's data tables
#      are created by the smoke setup script from DDL, "test-scope; 005/007
#      own production DDL". This module therefore only COMPUTES the lake
#      db's name locally (`local.lake_glue_database`, same naming-grammar
#      derivation `modules/spine-platform` itself uses for the spine db) to
#      build Glue Catalog ARNs for IAM -- it never creates the database
#      resource, and IAM statements referencing a not-yet-existing Glue
#      database/table ARN are valid Terraform (ARNs are string patterns,
#      not existence-checked at apply time).
#   2. Glue job `command.script_location` must point at an S3-hosted .py
#      file Glue itself executes (a hard Glue API requirement distinct from
#      the wheel). Two options considered: (a) have Terraform generate and
#      upload the thin launcher inline via `aws_s3_object` with heredoc
#      content; (b) accept it as a content-addressed, deploy-pushed
#      variable alongside the wheel (I-23's own pattern). Chose (b),
#      `var.glue_entrypoint_script_uri` -- the launcher is a stable 3-line
#      shim (`from spine.entrypoints.glue_main import main; main(sys.argv
#      [1:])`) whose only real dependency is the wheel's own `main(argv)`
#      surface, so versioning it alongside the wheel under the same
#      git-sha prefix keeps one content-addressed, immutable deploy unit
#      instead of splitting script content into HCL (which would drift
#      from the actual `spine.entrypoints.glue_main` signature silently).
#      The deploy pipeline (`make -C spine push-wheel`, out of this bead's
#      scope -- no Makefile edits) must push this launcher next to the
#      wheel; documented, not built, here.
#   3. `dlq` was listed among the platform inputs this module's variables
#      should accept, but nothing in this module's resource set (Glue job /
#      state machine / per-pipeline alarms) has a DLQ target to wire -- the
#      spine DLQ is entirely a `spine-platform`-level concern (the router's
#      own async-invocation DLQ and the bus rule's retry policy). No DLQ
#      variable is declared; noted as a deviation from the literal bullet
#      list, not an omission.
#   4. Alerting: `modules/spine-platform` creates its OWN conditional SNS
#      topic (`${p}-spine-alerts`) but does not output its ARN, so this
#      module cannot reuse it without touching that module. Mirrors the
#      established repo idiom instead (ingestion `modules/platform` and
#      `modules/spine-platform` each own an independent optional
#      `alert_email` -> SNS topic): this module takes its own
#      `var.alert_email` and owns a per-pipeline topic if set.

terraform {
  required_version = ">= 1.7"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.60"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  # `${p}` throughout the LLD == `${name_prefix}-${env}` (S5).
  p = "${var.name_prefix}-${var.env}"

  # S5: "Pipeline slug | slug(pipeline) = pipeline with / -> --." Mirrors
  # `modules/feed`'s own `local.slug = replace(var.feed.feed_id, "/", "--")`
  # pattern; the injective-grammar PROOF (unslug round-trip) is a Python
  # property test (naming.py, S5/S12.4) -- this module's `validation` block
  # below re-checks the grammar itself (not injectivity, which is a
  # property over the whole function, not a single value) before any
  # ARN/name composition, per S5's own instruction. USED for ARN/name/
  # execution-path composition ONLY (job name, IAM role name) -- NOT for
  # any Iceberg/Glue TABLE identifier (see `local.table_slug` immediately
  # below for why).
  slug = replace(var.pipeline, "/", "--")

  # F-1 fix (security gate `wf_c9aadeb2-8eb`, MEDIUM): the pipeline's own
  # TRAILING `/`-segment -- mirrors `core/naming.py::table_slug` EXACTLY
  # (`pipeline.rsplit("/", 1)[-1]`), the canonical slug for composing any
  # Iceberg/Glue TABLE identifier (004.1 S5's own naming table names ONE
  # slug function for both the ARN/exec-path use above AND the literal
  # `<slug>__raw` table-name row -- an erratum against that table, per
  # `naming.table_slug`'s own docstring: `local.slug`'s "--"-joined form is
  # actively dangerous as an UNQUOTED Iceberg/Spark-SQL identifier, since
  # "--" opens a line comment there, silently truncating everything after
  # it). The only deployed pipeline (`envs/dev/main.tf`'s
  # `local.identity_pipeline = "pipelines/identity"`) is multi-segment, so
  # this is not a theoretical gap: `local.slug` would have produced
  # `pipelines--identity__raw` etc. -- a table name the runner
  # (`core/naming.py`, whose own `raw_table`/`fact_table`/... fields the
  # deployed spec authors as bare `identity__raw` etc.) never resolves,
  # an unconditional first-deploy AccessDenied. `tests/unit/
  # test_pipeline_table_grants_wiring.py` asserts this local's derivation
  # agrees with `naming.table_slug` for both a multi- and single-segment
  # probe.
  table_slug = element(split("/", var.pipeline), length(split("/", var.pipeline)) - 1)

  # S5: "State machine | ${p}-spine-<slug>"; "Glue job | ${p}-spine-<slug>"
  # -- the SAME name for both resources (different resource types, no
  # collision).
  job_name = "${local.p}-spine-${local.slug}"

  account_id = data.aws_caller_identity.current.account_id

  glue_catalog_arn = "arn:aws:glue:${var.region}:${local.account_id}:catalog"

  # Ambiguity 1 (see file header): computed, not created, here.
  lake_glue_database = "${var.name_prefix}_${var.env}_lake"

  lake_database_arn  = "arn:aws:glue:${var.region}:${local.account_id}:database/${local.lake_glue_database}"
  spine_database_arn = "arn:aws:glue:${var.region}:${local.account_id}:database/${var.spine_database_name}"

  # S5: "Data tables ... tables <slug>__raw, <slug>__quarantine,
  # <slug>__facts, <slug>__state" -- all four, in the lake db, PER-TABLE
  # ARNs only (I-21/S-5: "never database-wide"). Plus `<slug>__markers`
  # (007.1 S6.3/S6.5, `core/naming.py::markers_table`) -- the fifth,
  # DERIVED (never authored) per-pipeline table: the commit/bind marker
  # table `effects/spark.py::_require_marker_table`/`append_marker_row`/
  # `read_marker_completions`/`read_marker_presence` and
  # `entrypoints/glue_main.py::_committed_tables` all read or write.
  # Critique finding N1 (gate wf_a0ef7f3b-6aa, bead conveyer-swb.28,
  # MAJOR): this table had NO Glue-catalog grant at all -- 007.1 S6.3's
  # "own-slug prefix => zero new IAM objects" claim was false against this
  # module (pre-existing, predates this bead's own changes; the S6.3 text
  # itself is erratum'd separately, not edited here). `spine/tests/unit/
  # test_pipeline_table_grants_wiring.py` derives the expected DERIVED
  # suffix directly from `core.naming.markers_table` (never hardcodes the
  # literal "__markers" string) and fails CI if a future naming.py-derived
  # table suffix is ever added here without a matching Terraform grant.
  #
  # F-1 fix: uses `local.table_slug` (the TABLE-NAME slug), NOT `local.
  # slug` -- see `local.table_slug`'s own comment above. "<slug>" in S5's
  # literal text is THIS derivation for a table-name row, not the ARN/
  # exec-path one.
  pipeline_table_names = [
    "${local.table_slug}__raw",
    "${local.table_slug}__quarantine",
    "${local.table_slug}__facts",
    "${local.table_slug}__state",
    "${local.table_slug}__markers",
  ]
  pipeline_table_arns = [
    for t in local.pipeline_table_names :
    "arn:aws:glue:${var.region}:${local.account_id}:table/${local.lake_glue_database}/${t}"
  ]
  run_ledger_table_arn = "arn:aws:glue:${var.region}:${local.account_id}:table/${var.spine_database_name}/${var.run_ledger_table_name}"

  glue_job_arn = "arn:aws:glue:${var.region}:${local.account_id}:job/${local.job_name}"

  # I-18/T-2: "SFN TimeoutSeconds = (MaxAttempts+1) * sla_minutes * 60 + 900"
  # -- MaxAttempts=2 is hardcoded by the S8.2 Retry block below (verbatim
  # template), not a variable, so this arithmetic and that block must
  # change together.
  sfn_max_attempts    = 2
  sfn_timeout_seconds = (local.sfn_max_attempts + 1) * var.sla_minutes * 60 + 900

  # T-16: mirrors `spine/entrypoints/glue_main.py::_catalog_conf` verbatim
  # (same keys, same `type=glue` value for the glue branch, no extra
  # `warehouse` property -- the code doesn't set one for `type=glue`
  # either, and this module's job is to double-set what the code sets, not
  # invent additional catalog conf). Required because the code's own
  # `SparkSession.builder.config(...)` calls silently no-op against an
  # ALREADY-active session -- verified empirically for the local test
  # session (agent-memory `spine-glue-main-entrypoint-di-seams.md`
  # finding 6) -- and Glue's `glueetl` job type may pre-create a
  # SparkContext/SparkSession before this job's script ever runs. Glue's
  # own multi-value `--conf` convention (repeat `--conf k=v` inside ONE
  # argument string) is used because Glue job arguments are a flat
  # string-to-string map -- there is no separate multi-valued `--conf` key.
  iceberg_conf = join(" --conf ", [
    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.spine_cat=org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.spine_cat.type=glue",
    "spark.sql.adaptive.enabled=true",
  ])
}
