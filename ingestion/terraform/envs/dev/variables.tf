# LLD S10.1: "Root variables: name_prefix (default conveyer), env, region,
# image_uri, alert_email (default ""), landing_glacier_days (default 90),
# driver_bytes_budget (default 5368709120)."

variable "name_prefix" {
  description = "First half of \"$${p}\" throughout the LLD (S5): all physical resource names derive from \"$${name_prefix}-$${env}\"."
  type        = string
  default     = "conveyer"
}

variable "env" {
  description = "Second half of \"$${p}\" (S5), e.g. \"dev\"."
  type        = string
}

variable "region" {
  description = "AWS region for every resource in this env."
  type        = string
}

variable "image_uri" {
  description = "Container image URI (\"<ecr>:<tag>\") shared by every Lambda function (D-2). See the deployment runbook, S10.8: the ECR repo must exist and be pushed to before this can point at a real tag."
  type        = string
}

variable "alert_email" {
  description = "Optional email for the platform's SNS alarm topic (S11.3). Empty disables the SNS subscription."
  type        = string
  default     = ""
}

variable "landing_glacier_days" {
  description = "Days before canonical landing data transitions to GLACIER_IR (S10.2). Canonical data is never expired, only transitioned -- verbatim-forever (arch S12)."
  type        = number
  default     = 90
}

variable "driver_bytes_budget" {
  description = "Default per-run acquisition byte budget for sftp-pull drivers, in bytes (S9.2 step 5; default 5 GiB). Wired to each sftp-pull driver's CONVEYER_DRIVER_BYTES_BUDGET env var."
  type        = number
  default     = 5368709120
}

# --- Runner Spine (LLD 004.1 D-1): this root also owns modules/spine-
# platform + one modules/spine-pipeline per pipeline (Phase 1: exactly the
# identity exemplar). See main.tf's "spine" section for the module wiring
# and the artifacts-bucket-policy-merge / operator-role decisions (004.1
# S10.1/S10.3, this bead's own handoff notes).

variable "spine_router_zip_path" {
  description = <<-EOT
    Path to the zip `make -C spine package-router` builds (I-8). Its
    `filebase64sha256` is evaluated at PLAN time (not just apply) by
    `modules/spine-platform`'s router Lambda resource, so the file must
    already exist on disk before `plan`/`apply` -- runbook step: build
    before plan (spine/README.md's runbook, 004.1 S10.6).
  EOT
  type        = string
  default     = "../../../../spine/dist/router.zip"
}

variable "spine_argv_budget_bytes" {
  description = "CONVEYER_ARGV_BUDGET_BYTES for the router (S8.2 [T-5]); default mirrors the router's own hardcoded fallback."
  type        = number
  default     = 8192
}

variable "spine_alert_email" {
  description = "Email subscribed to the spine platform's alarm SNS topic; empty disables SNS (mirrors ingestion's own alert_email, kept as a separate var since the two topics are independent, modules/spine-platform's own ambiguity 4)."
  type        = string
  default     = ""
}

variable "spine_log_retention_days" {
  description = "CloudWatch Logs retention for the router Lambda's + the identity pipeline job's explicitly-created log groups [S-18]."
  type        = number
  default     = 30
}

variable "spine_wheel_uri" {
  description = <<-EOT
    Content-addressed spine wheel key (I-23): `s3://$${p}-artifacts/spine/
    <git-sha>/conveyer_spine-<version>-py3-none-any.whl`. This is a
    DEPLOY-TIME ARTIFACT -- the default below is a PLACEHOLDER (this var is
    a plain string, never existence-checked at plan time, unlike the
    router zip above): `make -C spine push-wheel ENV=dev` prints the real
    URI after `make -C spine package`; pass it via `-var
    spine_wheel_uri=<printed URI>` on `apply` (spine/README.md's runbook).
  EOT
  type        = string
  default     = "s3://conveyer-dev-artifacts/spine/PLACEHOLDER-git-sha/conveyer_spine-0.1.0-py3-none-any.whl"
}

variable "glue_entrypoint_script_uri" {
  description = <<-EOT
    S3 location of the thin Glue driver script `command.script_location`
    executes (a hard Glue API requirement distinct from the wheel --
    `modules/spine-pipeline`'s own "ambiguity 2", main.tf header).
    DEPLOY-TIME ARTIFACT, deliberately not yet built by any Makefile target
    (documented, not built, by the bead that added `modules/spine-
    pipeline`) -- the placeholder default below must be overridden with a
    real pushed 3-line shim (`import sys; from spine.entrypoints.glue_main
    import main; main(sys.argv[1:])`) before a real job run can succeed;
    `plan`/`validate` never existence-check this (plain string var).
  EOT
  type        = string
  default     = "s3://conveyer-dev-artifacts/spine/PLACEHOLDER-git-sha/glue_driver.py"
}

variable "spine_pipeline_spec_uri" {
  description = "`s3://$${p}-artifacts/spine/specs/identity/pipeline.yaml` (I-23 allowlist root) for the identity exemplar. Deploy-time artifact, same placeholder posture as the wheel/entrypoint-script vars above."
  type        = string
  default     = "s3://conveyer-dev-artifacts/spine/specs/identity/pipeline.yaml"
}

variable "artifacts_deploy_principal_arn" {
  description = <<-EOT
    The single principal excepted from `modules/spine-platform`'s
    spine/*-prefix PutObject/DeleteObject* deny (I-23) -- must be whoever
    (or whatever CI role) runs `push-wheel`/uploads specs.

    REQUIRED, no default (conveyer-nvh.47 fix): this MUST be the ROLE ARN
    (`arn:aws:iam::<account>:role/<role-name>`), never a session/assumed-
    role ARN (`arn:aws:sts::<account>:assumed-role/<role-name>/<session>`)
    and never a plain IAM user ARN unless the deploy principal genuinely is
    a user. The deny's own condition (`modules/spine-platform/s3.tf`) tests
    `aws:PrincipalArn`, and AWS's own documented behavior for that condition
    key is: for a request made by an assumed role, `aws:PrincipalArn`
    evaluates to the ROLE's ARN, NOT the session ARN the caller actually
    authenticated as. This variable previously defaulted to
    `data.aws_caller_identity.current.arn` when left empty -- for anyone
    applying as an assumed role (the overwhelmingly common CI/deploy case),
    `aws_caller_identity.current.arn` itself resolves to the SESSION arn
    (`assumed-role/<role>/<session-name>`), a value `aws:PrincipalArn`
    never actually presents. The `StringNotLike` deny-exception therefore
    matched NOBODY -- not even the real deploying identity -- so the deny
    fired unconditionally on every `spine/*` write, including the deploy's
    own `push-wheel`/spec-upload calls (fails closed, but closed against
    everyone). Fix: no fallback: an explicit, correctly-shaped role ARN is
    required so the exception can ever actually except anyone.
  EOT
  type        = string
}

variable "spine_operator_principal_arns" {
  description = <<-EOT
    LLD 004.1 S10.3: the ONLY principals (besides the router role) allowed
    `states:StartExecution` on the spine state machines, for I-20's
    governed `--rN` deliberate-rerun path. Default empty -- the operator
    role's trust condition (main.tf) can never be satisfied by an empty
    list, so the role exists (states:StartExecution is granted) but NO ONE
    can assume it until an operator's real principal ARN is wired in here
    (document-and-defer per this bead's task framing; a real value is an
    operational decision, not a code one).
  EOT
  type        = list(string)
  default     = []
}
