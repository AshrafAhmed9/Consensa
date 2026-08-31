TLA_JAR ?= /tmp/consensa-tla2tools.jar
JAVA ?= java

.PHONY: check
check:
	@test -f $(TLA_JAR) || (echo "TLA_JAR not found at $(TLA_JAR) -- set TLA_JAR=/path/to/tla2tools.jar" && exit 1)
	@echo "--- membership.tla (correct: dual majority) ---"
	$(JAVA) -XX:+UseParallelGC -cp $(TLA_JAR) tlc2.TLC -deadlock membership.tla
	@echo "--- membership_broken.tla (must FAIL: single-majority OR) ---"
	@if $(JAVA) -XX:+UseParallelGC -cp $(TLA_JAR) tlc2.TLC -deadlock membership_broken.tla; then \
		echo "ERROR: membership_broken.tla passed TLC -- it is supposed to fail. The negative control is broken." && exit 1; \
	else \
		echo "membership_broken.tla failed as expected -- the checker is discriminating."; \
	fi
	@echo "--- split.tla (recursive splitting) ---"
	$(JAVA) -XX:+UseParallelGC -cp $(TLA_JAR) tlc2.TLC -deadlock split.tla
	@rm -rf states *_TTrace_*.tla *_TTrace_*.bin

.PHONY: clean
clean:
	@rm -rf states *_TTrace_*.tla *_TTrace_*.bin
