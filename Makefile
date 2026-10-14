# The jobs.v1 contract lives in the shared buf.build/ogen-app/proto module
# (CON-220). `make proto` generates from a pinned version of that module; bump
# PROTO_VERSION to adopt a new contract, then commit gen/. Before a version is
# published, generate from a local checkout:
#   make proto PROTO_SOURCE=../proto/proto
PROTO_MODULE  = buf.build/ogen-app/proto
PROTO_VERSION = v1.13.0
PROTO_SOURCE ?= $(PROTO_MODULE):$(PROTO_VERSION)

TEST_PG_CONTAINER = river-service-test-pg
TEST_DATABASE_DSN ?= postgres://river:river@localhost:5434/postgres?sslmode=disable

.PHONY: build run test test-db lint proto tidy docker

build:
	go build -o river-service ./cmd/river-service

test:
	go test -race ./...

# Runs the database-backed tests against a throwaway Postgres; the container
# and its volume are removed afterwards.
test-db:
	@docker rm -f -v $(TEST_PG_CONTAINER) >/dev/null 2>&1 || true
	@docker run -d --name $(TEST_PG_CONTAINER) -e POSTGRES_USER=river -e POSTGRES_PASSWORD=river \
		-e POSTGRES_DB=postgres -p 5434:5432 postgres:17 >/dev/null
	@until docker exec $(TEST_PG_CONTAINER) pg_isready -U river >/dev/null 2>&1; do sleep 1; done
	@TEST_DATABASE_DSN="$(TEST_DATABASE_DSN)" go test -race ./...; EXIT=$$?; \
		docker rm -f -v $(TEST_PG_CONTAINER) >/dev/null 2>&1; exit $$EXIT

lint:
	gofmt -l $$(git ls-files '*.go' | grep -v '^gen/')
	go vet ./...

proto:
	buf generate $(PROTO_SOURCE) --path jobs/v1/jobs.proto

tidy:
	go mod tidy

docker:
	docker build -t river-service .
