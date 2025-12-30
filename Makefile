IMAGE ?= ghcr.io/dbpiper/logstream:latest
CARGO ?= cargo
HEADLAMP_VERSION ?= 0.1.36
HEADLAMP_FORCE ?= 0
TOOLS_DIR ?= .cargo-bin
HEADLAMP_BIN ?= $(TOOLS_DIR)/bin/headlamp
HEADLAMP_STAMP ?= $(TOOLS_DIR)/.headlamp-$(HEADLAMP_VERSION).installed
CARGO_REGISTRY_DIR ?= /usr/local/cargo/registry
CARGO_GIT_DIR ?= /usr/local/cargo/git

build:
	$(CARGO) build --release

tools: $(HEADLAMP_STAMP)

$(HEADLAMP_STAMP):
	@FORCE_FLAG=""; \
	if [ "$(HEADLAMP_FORCE)" = "1" ]; then FORCE_FLAG="--force"; fi; \
	$(CARGO) install headlamp --version $(HEADLAMP_VERSION) --locked --root $(TOOLS_DIR) $$FORCE_FLAG
	@touch "$(HEADLAMP_STAMP)"

headlamp-update:
	@[ -n "$(VERSION)" ] || (echo "usage: make headlamp-update VERSION=x.y.z" && exit 2)
	@$(MAKE) tools HEADLAMP_VERSION="$(VERSION)" HEADLAMP_FORCE=1

test:
	@([ -x "$(HEADLAMP_BIN)" ] || command -v headlamp >/dev/null 2>&1) || ( \
		echo "error: headlamp is required to run tests"; \
		echo "hint: run 'make tools' (installs $(HEADLAMP_BIN))"; \
		exit 127; \
	)
	@HEADLAMP_ACTUAL_BIN="$(HEADLAMP_BIN)"; \
	if [ ! -x "$$HEADLAMP_ACTUAL_BIN" ]; then HEADLAMP_ACTUAL_BIN="$$(command -v headlamp)"; fi; \
	"$$HEADLAMP_ACTUAL_BIN" --runner=cargo-nextest --release --features testing

test-dev:
	@([ -x "$(HEADLAMP_BIN)" ] || command -v headlamp >/dev/null 2>&1) || ( \
		echo "error: headlamp is required to run tests"; \
		echo "hint: run 'make tools' (installs $(HEADLAMP_BIN))"; \
		exit 127; \
	)
	@HEADLAMP_ACTUAL_BIN="$(HEADLAMP_BIN)"; \
	if [ ! -x "$$HEADLAMP_ACTUAL_BIN" ]; then HEADLAMP_ACTUAL_BIN="$$(command -v headlamp)"; fi; \
	"$$HEADLAMP_ACTUAL_BIN" --runner=cargo-nextest --changed=branch --onlyFailures --release --features testing

test-ci:
	@([ -x "$(HEADLAMP_BIN)" ] || command -v headlamp >/dev/null 2>&1) || ( \
		echo "error: headlamp is required to run tests"; \
		echo "hint: run 'make tools' (installs $(HEADLAMP_BIN))"; \
		exit 127; \
	)
	@HEADLAMP_ACTUAL_BIN="$(HEADLAMP_BIN)"; \
	if [ ! -x "$$HEADLAMP_ACTUAL_BIN" ]; then HEADLAMP_ACTUAL_BIN="$$(command -v headlamp)"; fi; \
	"$$HEADLAMP_ACTUAL_BIN" --runner=cargo-nextest --changed=lastCommit --ci --features testing --cargo-profile ci

fmt:
	$(CARGO) fmt --all

clippy:
	$(CARGO) clippy --all-targets --features testing -- -D warnings

docker-build:
	docker build \
		--build-arg CARGO_REGISTRY_DIR=$(CARGO_REGISTRY_DIR) \
		--build-arg CARGO_GIT_DIR=$(CARGO_GIT_DIR) \
		-t $(IMAGE) .

docker-push:
	docker push $(IMAGE)
