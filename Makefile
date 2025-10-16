APP=server

.PHONY: run build docker test

run:
	go run ./cmd/server

build:
	go build -o bin/$(APP) ./cmd/server

docker:
	docker build -t social-graph:latest .

test:
	go test ./...
