# Conveyer monorepo — targets fan out to each module's Makefile.
# `make lint test` = ingestion + spine + linter on both + contracts both
# sides (LLD 004.1 §12.7); spine's own Makefile no-ops `registry`/
# `test-golden` (ingestion-only concepts) so this generic fan-out stays a
# flat target list instead of per-module target-aware.
MODULES := ingestion spine

.PHONY: setup lint schemas registry test test-unit test-golden test-integration test-contracts

setup lint schemas registry test test-unit test-golden test-integration test-contracts:
	@for m in $(MODULES); do $(MAKE) -C $$m $@ || exit 1; done
