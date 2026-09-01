BIN := bin
GO  ?= go

.PHONY: all build test test-race vet lint fmt tidy clean run backend check demo demo-down

all: vet test build

build:
	$(GO) build -o $(BIN)/manifold ./cmd/manifold
	$(GO) build -o $(BIN)/backend  ./bench/backend

test:
	$(GO) test ./... -count=1

# The race detector needs cgo and a C toolchain, which the Windows dev box does
# not have. Run this in WSL2 or rely on CI, which runs it on ubuntu-latest.
test-race:
	CGO_ENABLED=1 $(GO) test ./... -race -count=1

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

tidy:
	$(GO) mod tidy

lint:
	golangci-lint run

# Validate the example config without starting a listener.
check: build
	./$(BIN)/manifold -config config.example.yaml -check

run: build
	./$(BIN)/manifold -config config.example.yaml

backend: build
	./$(BIN)/backend -addr :9001 -latency 1ms

clean:
	rm -rf $(BIN)

# One-command demo: three chaos-controllable backends, manifold, Prometheus.
# See deploy/README.md for the five-minute walkthrough.
demo:
	docker compose -f deploy/docker-compose.yml up --build -d

demo-down:
	docker compose -f deploy/docker-compose.yml down -v
