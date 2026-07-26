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
