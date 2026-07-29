# conveyer-ingestion — make targets per LLD §12.6 (CI-agnostic; any CI calls these).
# CI = `make lint schemas registry test coverage`; that sequence green is the definition of mergeable.
UV := uv run

.PHONY: setup lint schemas registry test test-unit test-golden test-integration test-contracts \
        coverage package push-image bootstrap-ledger plan apply put-secret smoke

setup:
	uv sync --all-packages

lint:
	$(UV) ruff check .
	$(UV) ruff format --check .
	$(UV) mypy
	$(UV) python ../tools/purity_linter.py ingestion  # promoted engine, config-as-data (LLD 004.1 I-15/§12.3)

schemas:  ## regenerate contracts/*.json from core/model.py; fail on drift (LLD §6)
	$(UV) python -m tools.export_schemas  # M0 deliverable
	git diff --exit-code -- contracts/

registry:  ## validate + render sources/**/source.yaml locally (LLD §6.8, D-12)
	$(UV) python -m tools.render_registry  # M0 deliverable

test-unit:
	$(UV) pytest tests/unit

test-golden:
	$(UV) pytest tests/golden

test-integration:
	$(UV) pytest tests/integration

test-contracts:  ## producer-side exactness against contracts/fixtures/events/** (LLD 004.1 I-16)
	$(UV) pytest tests/contracts

test: test-unit test-golden test-integration test-contracts

# LLD §12.5 gate: core/ >= 95% branch; overall >= 85% (`pyproject.toml`'s
# [tool.coverage.run] sets `branch = true` + the one whole-file adapter
# exclusion, `effects/sftp.py` -- see that file's own module docstring).
coverage:
	$(UV) pytest tests/unit tests/golden tests/integration --cov=ingestion --cov-branch --cov-report=term-missing
	$(UV) coverage report --fail-under=85
	$(UV) coverage report --include='ingestion/core/*' --fail-under=95

package:
	docker build -t conveyer-ingestion:$$(git rev-parse --short HEAD) .

push-image: package  ## needs ENV; docker tag+push to ECR, tag = git SHA (LLD §5/§10.5/§10.8 step 4)
	@test -n "$(ENV)" || { echo "ENV is required"; exit 1; }
	@region="$${AWS_REGION:-$${AWS_DEFAULT_REGION:-$$(aws configure get region)}}"; \
	test -n "$$region" || { echo "AWS_REGION (or \`aws configure set region\`) is required"; exit 1; }; \
	account_id="$$(aws sts get-caller-identity --query Account --output text)"; \
	test -n "$$account_id" || { echo "failed to resolve AWS account id -- check AWS credentials"; exit 1; }; \
	name_prefix="$${CONVEYER_NAME_PREFIX:-conveyer}"; \
	registry="$$account_id.dkr.ecr.$$region.amazonaws.com"; \
	repo="$$registry/$$name_prefix-$(ENV)-ingestion"; \
	tag="$$(git rev-parse --short HEAD)"; \
	aws ecr get-login-password --region "$$region" | docker login --username AWS --password-stdin "$$registry" && \
	docker tag conveyer-ingestion:$$tag "$$repo:$$tag" && \
	docker push "$$repo:$$tag" && \
	echo "pushed $$repo:$$tag"

bootstrap-ledger:  ## idempotent Iceberg table creation (LLD §6.2)
	@test -n "$(ENV)" || { echo "ENV is required"; exit 1; }
	$(UV) python -m ingestion.bootstrap.create_ledger --env $(ENV)

plan:
	@test -n "$(ENV)" || { echo "ENV is required"; exit 1; }
	terraform -chdir=terraform/envs/$(ENV) plan

apply:
	@test -n "$(ENV)" || { echo "ENV is required"; exit 1; }
	terraform -chdir=terraform/envs/$(ENV) apply

put-secret:  ## prompts for the SFTP secret JSON (LLD §6.7); never echoes
	@test -n "$(ENV)" || { echo "ENV is required"; exit 1; }
	@test -n "$(FEED)" || { echo "FEED is required"; exit 1; }
	$(UV) python -m tools.put_secret --feed $(FEED) --env $(ENV)

smoke:  ## post-deploy end-to-end probe against real dev AWS (LLD §12.5) — M6
	@test -n "$(ENV)" || { echo "ENV is required"; exit 1; }
	$(UV) pytest tests/smoke --env $(ENV)
