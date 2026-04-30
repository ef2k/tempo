.PHONY: install test race bench

install:
	go install gotest.tools/gotestsum@v1.13.0

test:
	gotestsum -- ./...

race:
	gotestsum -- -race ./...

bench:
	go test -run '^$$' -bench . -benchmem
