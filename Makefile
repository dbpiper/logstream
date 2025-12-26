IMAGE ?= ghcr.io/dbpiper/logstream:latest
CARGO ?= cargo
HEADLAMP_VERSION ?= 0.1.35
TOOLS_DIR ?= .cargo-bin
HEADLAMP_BIN ?= $(TOOLS_DIR)/bin/headlamp
CARGO_REGISTRY_DIR ?= /usr/local/cargo/registry
CARGO_GIT_DIR ?= /usr/local/cargo/git

build:
	$(CARGO) build --release

tools: $(HEADLAMP_BIN)

$(HEADLAMP_BIN):
	$(CARGO) install headlamp --version $(HEADLAMP_VERSION) --locked --root $(TOOLS_DIR)

test:
	@([ -x "$(HEADLAMP_BIN)" ] || command -v headlamp >/dev/null 2>&1) || ( \
		echo "error: headlamp is required to run tests"; \
		echo "hint: run 'make tools' (installs $(HEADLAMP_BIN))"; \
		exit 127; \
	)
	@HEADLAMP_ACTUAL_BIN="$(HEADLAMP_BIN)"; \
	if [ ! -x "$$HEADLAMP_ACTUAL_BIN" ]; then HEADLAMP_ACTUAL_BIN="$$(command -v headlamp)"; fi; \
	"$$HEADLAMP_ACTUAL_BIN" --runner=cargo-test --release --features testing

test-dev:
	@([ -x "$(HEADLAMP_BIN)" ] || command -v headlamp >/dev/null 2>&1) || ( \
		echo "error: headlamp is required to run tests"; \
		echo "hint: run 'make tools' (installs $(HEADLAMP_BIN))"; \
		exit 127; \
	)
	@HEADLAMP_ACTUAL_BIN="$(HEADLAMP_BIN)"; \
	if [ ! -x "$$HEADLAMP_ACTUAL_BIN" ]; then HEADLAMP_ACTUAL_BIN="$$(command -v headlamp)"; fi; \
	"$$HEADLAMP_ACTUAL_BIN" --runner=cargo-test --changed=branch --onlyFailures --release --features testing

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
