.DEFAULT_GOAL := build

.PHONY: build

# Bootstrap the single CLI. All other commands are available through bin/kpl.
build:
	go build -o bin/kpl ./cmd/kpl-master
