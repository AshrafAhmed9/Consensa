TLA_JAR ?= tla2tools.jar
JAVA ?= java

.PHONY: check
check:
	@test -f "$(TLA_JAR)" || (echo "TLA_JAR not found at $(TLA_JAR) -- set TLA_JAR=/path/to/tla2tools.jar" && exit 1)
	@echo "--- membership.tla ---"
	$(JAVA) -XX:+UseParallelGC -cp "$(TLA_JAR)" tlc2.TLC -deadlock membership.tla
	@echo "--- split.tla (recursive splitting) ---"
	$(JAVA) -XX:+UseParallelGC -cp "$(TLA_JAR)" tlc2.TLC -deadlock split.tla
	@rm -rf states *_TTrace_*.tla *_TTrace_*.bin

.PHONY: clean
clean:
	@rm -rf states *_TTrace_*.tla *_TTrace_*.bin
