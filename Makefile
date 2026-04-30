.PHONY: install test race bench stress soak soak-slow soak-long clean

TEST_PACKAGES := $(filter-out github.com/ef2k/tempo/performance,$(shell go list ./...))

install:
	go install gotest.tools/gotestsum@v1.13.0

test:
	gotestsum -- $(TEST_PACKAGES)

race:
	gotestsum -- -race $(TEST_PACKAGES)

bench:
	go test ./performance -run '^$$' -bench . -benchmem

stress:
	TEMPO_RUN_STRESS=1 gotestsum -- -run '^TestStressHighConcurrencyDelivery$$' ./performance

soak: SOAK_DURATION ?= 5m
soak:
	TEMPO_RUN_SOAK=1 TEMPO_SOAK_DURATION=$(SOAK_DURATION) go test -timeout 20m -v -run '^TestSoakSustainedLoadStaysHealthy$$' ./performance

soak-slow: SOAK_DURATION ?= 5m
soak-slow:
	TEMPO_RUN_SOAK=1 TEMPO_SOAK_DURATION=$(SOAK_DURATION) TEMPO_SOAK_CONSUMER_DELAY=100us go test -timeout 20m -v -run '^TestSoakSustainedLoadStaysHealthy$$' ./performance

soak-long:
	TEMPO_RUN_SOAK=1 TEMPO_SOAK_DURATION=30m go test -timeout 70m -v -run '^TestSoakSustainedLoadStaysHealthy$$' ./performance

clean:
	rm -rf performance/out
