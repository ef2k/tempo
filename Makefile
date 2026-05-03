.PHONY: install test race bench stress soak tune clean

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

tune: TUNE_DURATION ?= 15s
tune: TUNE_CONSUMER_DELAY ?=
tune:
	go run ./performance/cmd/tempo-tune -duration $(TUNE_DURATION) $(if $(TUNE_CONSUMER_DELAY),-consumer-delay $(TUNE_CONSUMER_DELAY),)

clean:
	rm -rf performance/out
