# conveyer-spine — make targets per LLD 004.1 §12.7.
# Every python-running recipe pins the interpreter to 3.11 (Glue 5.0 parity,
# I-1) via `uv run -p 3.11 --package conveyer-spine`, confining the pin to
# this Makefile so ad-hoc shell commands elsewhere don't need to know about
# it. Root CI = `make lint test` (root Makefile), spine parts under this pin.
UV := uv run -p 3.11 --package conveyer-spine

.PHONY: setup lint schemas test-unit test-frames test-integration test-contracts test \
        coverage registry test-golden package package-router push-wheel \
        bootstrap-ledger plan apply smoke

setup:
	uv sync --all-packages

lint:
	$(UV) ruff check .
	$(UV) ruff format --check .
	$(UV) mypy
	uv run python ../tools/purity_linter.py spine  # promoted engine, config-as-data (LLD 004.1 I-15/§12.3)

schemas:  ## regenerate spine/contracts/*.json from core/model.py; fail on drift (LLD §6, mirrors ingestion's pattern)
	$(UV) python -m tools.export_schemas
	git diff --exit-code -- contracts/

# `; code=$$?; test $$code -eq 0 -o $$code -eq 5` treats pytest's "no tests
# collected" (exit 5) as green -- M0 ships empty tests/{unit,frames,
# integration,contracts}/ dirs (real tests land M1+); any REAL failure
# (1/2/3/4) still fails the target. Once a milestone adds test files here,
# exit 5 stops being reachable and this is a no-op safety net, not a mask.
test-unit:
	$(UV) pytest tests/unit; code=$$?; test $$code -eq 0 -o $$code -eq 5

test-frames:
	$(UV) pytest tests/frames; code=$$?; test $$code -eq 0 -o $$code -eq 5

test-integration:
	$(UV) pytest tests/integration; code=$$?; test $$code -eq 0 -o $$code -eq 5

test-contracts:  ## consumer-side: every contracts/fixtures/events/**/*.json parses (LLD 004.1 I-16, R-10)
	$(UV) pytest tests/contracts; code=$$?; test $$code -eq 0 -o $$code -eq 5

test: test-unit test-frames test-integration test-contracts

# LLD §7.2 gate: core/ + frames/ >= 95% branch; overall >= 85%. Not part of
# `test` yet (test targets need not enforce coverage until real code lands,
# per §7.2) -- run explicitly once M1+ tests exist.
coverage:
	$(UV) pytest tests/unit tests/frames tests/integration tests/contracts --cov=spine --cov-branch --cov-report=term-missing
	$(UV) coverage report --fail-under=85
	$(UV) coverage report --include='spine/core/*,spine/frames/*' --fail-under=95

# no-op stubs: the root Makefile's generic fan-out (MODULES := ingestion
# spine) shares one target list across modules; these two are ingestion-only
# concepts (source registry rendering, golden pipeline tests) spine has no
# equivalent of. Keeping them here as harmless no-ops is simpler than making
# the root fan-out per-module target-aware, and costs nothing since neither
# is on the `make lint test` critical path.
registry:
	@:

test-golden:
	@:

# M5 deliverable (LLD §7.1, §8.1, I-8, I-23) -- not yet buildable in the
# sense of shipping a working artifact (no `spine.core.model`/entrypoints
# logic yet), but the packaging mechanics themselves work today against the
# M0 skeleton, so this is a real recipe, not a stub.
package:
	uv build -p 3.11 --package conveyer-spine --wheel --out-dir dist

# Zips exactly `spine/entrypoints/router.py` + `spine/core/naming.py` (plus
# the package `__init__.py`s needed to make `spine.entrypoints.router` and
# `spine.core.naming` importable) -- LLD §7.1, §8.1, I-8. An import test
# asserting the zip's transitive imports stay within stdlib + boto3 lands
# with the router itself (M5, bead conveyer-nvh.26).
package-router:
	rm -rf dist/router
	mkdir -p dist/router/spine/entrypoints dist/router/spine/core
	cp spine/__init__.py dist/router/spine/__init__.py
	cp spine/entrypoints/__init__.py dist/router/spine/entrypoints/__init__.py
	cp spine/entrypoints/router.py dist/router/spine/entrypoints/router.py
	cp spine/core/__init__.py dist/router/spine/core/__init__.py
	cp spine/core/naming.py dist/router/spine/core/naming.py
	cd dist/router && zip -r -q ../router.zip .
	@echo "built dist/router.zip"

# Content-addressed, immutable key (I-23): s3://<artifacts>/spine/<git-sha>/<wheel>.
# `make package` first (produces dist/*.whl). Real AWS required -- fails
# without credentials/bucket, which is expected pre-M6 (do not gate on it).
push-wheel:
	@test -n "$(ENV)" || { echo "ENV is required"; exit 1; }
	@sha="$$(git rev-parse HEAD)"; \
	wheel="$$(ls dist/*.whl 2>/dev/null | head -n1)"; \
	test -n "$$wheel" || { echo "no wheel in dist/ -- run 'make package' first"; exit 1; }; \
	name_prefix="$${CONVEYER_NAME_PREFIX:-conveyer}"; \
	bucket="$$name_prefix-$(ENV)-artifacts"; \
	key="spine/$$sha/$$(basename $$wheel)"; \
	aws s3 cp "$$wheel" "s3://$$bucket/$$key" && \
	echo "pushed s3://$$bucket/$$key"

bootstrap-ledger:  ## idempotent run_ledger Iceberg table creation (LLD §6.5, I-17)
	@test -n "$(ENV)" || { echo "ENV is required"; exit 1; }
	$(UV) python -m spine.bootstrap.create_run_ledger --env $(ENV)

# D-1: spine has no terraform env root of its own -- `modules/spine-platform`
# and `modules/spine-pipeline` are consumed from the existing ingestion env
# root (LLD §10.1: "extend the existing terraform/envs/dev root").
plan:
	@test -n "$(ENV)" || { echo "ENV is required"; exit 1; }
	terraform -chdir=../ingestion/terraform/envs/$(ENV) plan

apply:
	@test -n "$(ENV)" || { echo "ENV is required"; exit 1; }
	terraform -chdir=../ingestion/terraform/envs/$(ENV) apply

smoke:  ## post-deploy end-to-end probe against real dev AWS (LLD §12.1) -- M6
	@test -n "$(ENV)" || { echo "ENV is required"; exit 1; }
	$(UV) pytest tests/smoke --env $(ENV)
