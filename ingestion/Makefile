# conveyer-ingestion — make targets per LLD §12.6 (CI-agnostic; any CI calls these).
# CI = `make lint schemas registry test`; that sequence green is the definition of mergeable.
UV := uv run

.PHONY: setup lint schemas registry test test-unit test-golden test-integration \
        package push-image bootstrap-ledger plan apply put-secret smoke

setup:
	uv sync --all-packages

lint:
	$(UV) ruff check .
	$(UV) ruff format --check .
	$(UV) mypy
	$(UV) python tools/purity_linter.py  # M0 deliverable (LLD §12.2)

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

test: test-unit test-golden test-integration

package:
	docker build -t conveyer-ingestion:$$(git rev-parse --short HEAD) .

push-image: package  ## needs ENV; docker tag+push to ECR (LLD §10.8 step 4) — M6
	@test -n "$(ENV)" || { echo "ENV is required"; exit 1; }
	@echo "TODO(M6): ecr login + tag + push for ENV=$(ENV)"; exit 1

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
