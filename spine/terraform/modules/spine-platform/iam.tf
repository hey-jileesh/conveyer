# iam.tf -- the two platform-level roles from LLD S10.3's table:
# `spine-router` and `spine-sfn`. The per-pipeline `spine-job-<slug>` role
# (I-21) is a SIBLING `modules/spine-pipeline` concern -- not built here,
# by design (S10.3: "the resources are per-pipeline, so the role must be").
#
# Every trust policy below carries account/resource-scoped conditions
# (confused-deputy guard) [S-14], mirroring ingestion's `lambda_assume`/
# `scheduler_assume` shape (modules/platform/iam.tf).
#
# `iam:PassRole` is deliberately absent from both roles below (S10.3):
# `glue:startJobRun.sync` passes no role (the job definition itself carries
# its own); PassRole on `${p}-spine-*` roles is a deploy-principal-only,
# `iam:PassedToService = glue.amazonaws.com`-conditioned grant that belongs
# to the env root's deploy principal, not either runtime role here.
#
# Note on S10.3's operator-role mention ("states:StartExecution is also
# held by the named operator role for --rN runs -- no one else"): that
# operator role is an ENV-LEVEL identity (a human/automation principal for
# deliberate reruns, S10.6), not a spine-platform resource -- this module
# does not create it. Left to the env root to grant `states:StartExecution`
# on the same `${p}-spine-*` pattern to whatever operator principal it
# names; flagged in the bead handoff report as a decision, not an omission.

# --- trust policies ------------------------------------------------------

data "aws_iam_policy_document" "spine_router_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

data "aws_iam_policy_document" "spine_sfn_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = [local.spine_state_machine_arn_pattern]
    }
  }
}

# --- spine-router ----------------------------------------------------------

resource "aws_iam_role" "spine_router" {
  name               = "${local.p}-spine-router"
  assume_role_policy = data.aws_iam_policy_document.spine_router_assume.json
}

resource "aws_iam_role_policy_attachment" "spine_router_basic" {
  role       = aws_iam_role.spine_router.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "spine_router" {
  statement {
    sid       = "StartSpineExecutions"
    actions   = ["states:StartExecution"]
    resources = [local.spine_state_machine_arn_pattern]
  }

  statement {
    sid       = "DlqSend"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.spine_dlq.arn]
  }
}

resource "aws_iam_role_policy" "spine_router" {
  name   = "${local.p}-spine-router"
  role   = aws_iam_role.spine_router.id
  policy = data.aws_iam_policy_document.spine_router.json
}

# --- spine-sfn -------------------------------------------------------------
#
# Assumed by the Step Functions state machines' `.sync` Glue integration
# (S8.2) to StartJobRun + poll it. EXACTLY these four actions [S-17]: no
# `events:*` (publish happens job-side, inside the Glue job itself, under
# the per-pipeline job role -- I-21), no delete of any kind (S10.3's
# append-only posture restated: "no spine role holds any delete
# permission").

resource "aws_iam_role" "spine_sfn" {
  name               = "${local.p}-spine-sfn"
  assume_role_policy = data.aws_iam_policy_document.spine_sfn_assume.json
}

data "aws_iam_policy_document" "spine_sfn" {
  statement {
    sid = "GlueJobRunControl"
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun",
    ]
    resources = [local.spine_glue_job_arn_pattern]
  }
}

resource "aws_iam_role_policy" "spine_sfn" {
  name   = "${local.p}-spine-sfn"
  role   = aws_iam_role.spine_sfn.id
  policy = data.aws_iam_policy_document.spine_sfn.json
}
