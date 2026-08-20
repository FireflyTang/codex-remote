SHELL := /bin/bash

TOOLS_DIR := $(CURDIR)/.tools
BIN_DIR := $(TOOLS_DIR)/bin
GO_VERSION := 1.26.5
GO_ARCHIVE := go$(GO_VERSION).linux-amd64.tar.gz
GO_SHA256 := 5c2c3b16caefa1d968a94c1daca04a7ca301a496d9b086e17ad77bb81393f053
GO_ROOT := $(TOOLS_DIR)/go
GO := $(GO_ROOT)/bin/go
BUF_VERSION := 1.47.2
BUF_SHA256 := 3a0c4da8d46eea8136affa63db202c76a44f8112384160b73c3fffb1cf14b5d8
BUF := $(BIN_DIR)/buf
PROTOC_GEN_GO_VERSION := 1.36.11
PROTOC_GEN_GO := $(BIN_DIR)/protoc-gen-go

.PHONY: tools proto-lint proto-compile proto-generate generate test check clean-tools

tools: $(GO) $(BUF) $(PROTOC_GEN_GO)

$(GO):
	@mkdir -p $(TOOLS_DIR)
	curl -fL --retry 3 -o $(TOOLS_DIR)/$(GO_ARCHIVE) https://dl.google.com/go/$(GO_ARCHIVE)
	@printf '%s  %s\n' '$(GO_SHA256)' '$(TOOLS_DIR)/$(GO_ARCHIVE)' | sha256sum --check
	tar -C $(TOOLS_DIR) -xzf $(TOOLS_DIR)/$(GO_ARCHIVE)
	rm $(TOOLS_DIR)/$(GO_ARCHIVE)

$(BUF):
	@mkdir -p $(BIN_DIR)
	curl -fL --retry 3 -o $(BUF) https://github.com/bufbuild/buf/releases/download/v$(BUF_VERSION)/buf-Linux-x86_64
	@printf '%s  %s\n' '$(BUF_SHA256)' '$(BUF)' | sha256sum --check
	chmod +x $(BUF)

$(PROTOC_GEN_GO): $(GO) Makefile
	@mkdir -p $(BIN_DIR)
	GOBIN=$(BIN_DIR) $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@v$(PROTOC_GEN_GO_VERSION)

proto-lint: $(BUF)
	cd protocol && $(BUF) lint

proto-compile: $(BUF)
	cd protocol && $(BUF) build

proto-generate: $(BUF) $(PROTOC_GEN_GO)
	cd protocol && $(BUF) generate

generate: proto-generate

test: $(GO)
	$(GO) test ./...

check: proto-lint proto-compile proto-generate test

clean-tools:
	rm -rf $(TOOLS_DIR)
