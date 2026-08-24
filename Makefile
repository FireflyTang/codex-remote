SHELL := /bin/bash

TOOLS_DIR := $(CURDIR)/.tools
GO_VERSION := 1.26.5
GO_ARCHIVE := go$(GO_VERSION).linux-amd64.tar.gz
GO_SHA256 := 5c2c3b16caefa1d968a94c1daca04a7ca301a496d9b086e17ad77bb81393f053
GO_ROOT := $(TOOLS_DIR)/go
GO := $(GO_ROOT)/bin/go

.PHONY: tools protocol-check test check clean-tools

tools: $(GO)

$(GO):
	@mkdir -p $(TOOLS_DIR)
	curl -fL --retry 3 -o $(TOOLS_DIR)/$(GO_ARCHIVE) https://dl.google.com/go/$(GO_ARCHIVE)
	@printf '%s  %s\n' '$(GO_SHA256)' '$(TOOLS_DIR)/$(GO_ARCHIVE)' | sha256sum --check
	tar -C $(TOOLS_DIR) -xzf $(TOOLS_DIR)/$(GO_ARCHIVE)
	rm $(TOOLS_DIR)/$(GO_ARCHIVE)

protocol-check: $(GO)
	@set -eu; \
	lock_value() { awk -F'"' -v key="$$1" '$$2 == key { print $$4; exit }' protocol.lock; }; \
	module=$$(lock_value module); \
	version=$$(lock_value version); \
	repository=$$(lock_value repository); \
	commit=$$(lock_value commit); \
	descriptor_path=$$(lock_value path); \
	descriptor_sha=$$(lock_value sha256); \
	actual_version=$$($(GO) list -m -f '{{.Version}}' "$$module"); \
	test "$$actual_version" = "$$version" || { echo "protocol module version $$actual_version does not match lock $$version" >&2; exit 1; }; \
	download=$$($(GO) mod download -json "$$module@$$version"); \
	origin_url=$$(printf '%s\n' "$$download" | awk -F'"' '$$2 == "URL" { print $$4; exit }'); \
	origin_hash=$$(printf '%s\n' "$$download" | awk -F'"' '$$2 == "Hash" { print $$4; exit }'); \
	origin_ref=$$(printf '%s\n' "$$download" | awk -F'"' '$$2 == "Ref" { print $$4; exit }'); \
	test "$$origin_hash" = "$$commit" || { echo "protocol module origin commit $$origin_hash does not match lock $$commit" >&2; exit 1; }; \
	test "$$origin_ref" = "refs/tags/$$version" || { echo "protocol module origin ref $$origin_ref does not match expected tag refs/tags/$$version" >&2; exit 1; }; \
	test "$${origin_url%.git}" = "$${repository%.git}" || { echo "protocol module origin repository $$origin_url does not match lock $$repository" >&2; exit 1; }; \
	module_dir=$$($(GO) list -m -f '{{.Dir}}' "$$module"); \
	test -f "$$module_dir/$$descriptor_path" || { echo "protocol descriptor missing: $$module_dir/$$descriptor_path" >&2; exit 1; }; \
	actual_sha=$$(sha256sum "$$module_dir/$$descriptor_path" | awk '{ print $$1 }'); \
	test "$$actual_sha" = "$$descriptor_sha" || { echo "protocol descriptor SHA256 $$actual_sha does not match lock $$descriptor_sha" >&2; exit 1; }

test: $(GO)
	$(GO) test ./...

check: protocol-check test

clean-tools:
	rm -rf $(TOOLS_DIR)
