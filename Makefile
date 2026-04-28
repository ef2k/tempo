.PHONY: install test

install:
	go install gotest.tools/gotestsum@v1.13.0

test:
	gotestsum -- ./...
