name_prefix = "conveyer"
env         = "dev"
region      = "us-east-1"

# Placeholder until the ECR repo exists and an image has been pushed
# (runbook S10.8 steps 3-4: `apply -target=module.platform.aws_ecr_repository.ingestion`,
# then `make push-image ENV=dev`, then re-apply with the real tag).
image_uri = "000000000000.dkr.ecr.us-east-1.amazonaws.com/conveyer-dev-ingestion:latest"

alert_email = ""

landing_glacier_days = 90
driver_bytes_budget  = 5368709120

# conveyer-nvh.47: REQUIRED, no default -- must be the deploy ROLE's own ARN
# (arn:aws:iam::<account>:role/<role-name>), never an assumed-role SESSION
# arn (aws:PrincipalArn evaluates to the role arn for an assumed role, per
# variables.tf's own docstring). Placeholder below is enough for
# `validate`/`plan`; override with the real CI/deploy role before an
# `apply` that actually needs push-wheel/spec-upload to succeed.
artifacts_deploy_principal_arn = "arn:aws:iam::000000000000:role/conveyer-dev-deploy"
