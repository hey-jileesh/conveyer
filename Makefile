# Conveyer monorepo — targets fan out to each module's Makefile.
MODULES := ingestion

.PHONY: setup lint schemas registry test test-unit test-golden test-integration

setup lint schemas registry test test-unit test-golden test-integration:
	@for m in $(MODULES); do $(MAKE) -C $$m $@ || exit 1; done
