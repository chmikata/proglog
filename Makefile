.DEFAULT_GOAL := help
CONFIG_PATH := ${HOME}/.proglog/
PROTOBUF := /usr/local/protobuf

.PHONY: init
init:
	mkdir -p ${CONFIG_PATH}

.PHONY: gencert
gencert: ## Generate Certs
	cfssl gencert \
		-initca test/ca-csr.json | cfssljson -bare ca

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=server \
		test/server-csr.json | cfssljson -bare server

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="root" \
		test/client-csr.json | cfssljson -bare root-client

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="nobdy" \
		test/client-csr.json | cfssljson -bare nobody-client

	mv *.pem *.csr ${CONFIG_PATH}

$(CONFIG_PATH)/model.conf:
	cp test/model.conf $(CONFIG_PATH)/model.conf

$(CONFIG_PATH)/policy.csv:
	cp test/policy.csv $(CONFIG_PATH)/policy.csv

.PHONY: test
test: $(CONFIG_PATH)/model.conf $(CONFIG_PATH)/policy.csv ## Run go test
	go test -race ./...

.PHONY: setup
setup: grpcprotoc ## Setup tools.

.PHONY: grpcprotoc
grpcprotoc: ## Install Go-protobuf.
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
	go get -u google.golang.org/grpc

.PHONY: compile
compile: ## Compile protobuf with gRPC
	protoc api/v1/*.proto --go_out=. --go-grpc_out=. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative --proto_path=.

.PHONY: generate
generate: ## Generate codes
	go generate ./...

TAG := 0.0.1

.PHONY: build-docker-proglog
build-docker-proglog: ## Build Docker image
	docker build -t github.com/chmikata/proglog:$(TAG) -f ./Dockerfile-proglog .

.PHONY: build-docker-getservers
build-docker-getservers: ## Build Docker image
	docker build -t github.com/chmikata/getservers:$(TAG) -f ./Dockerfile-getservers .

help: ## Show options
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
