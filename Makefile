.DEFAULT_GOAL := help

PROTOBUF := /usr/local/protobuf
TMP := $(shell pwd)/tmp
DOCKER_TAG := latest

.PHONY: setup
setup: grpcprotoc ## Setup tools.

.PHONY: grpcprotoc
grpcprotoc: ## Install Go-protobuf.
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
	go get -u google.golang.org/grpc

.PHONY: compile
compile: ## Compile protobuf
	protoc api/v1/*.proto --go_out=. --go_opt=paths=source_relative --proto_path=.

.PHONY: test
test: ## Run go test
	go test -race ./...

.PHONY: generate
generate: ## Generate codes
	go generate ./...

help: ## Show options
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
