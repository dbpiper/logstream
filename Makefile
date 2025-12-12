IMAGE ?= ghcr.io/dbpiper/logstream:latest
CARGO ?= cargo
CARGO_REGISTRY_DIR ?= /usr/local/cargo/registry
CARGO_GIT_DIR ?= /usr/local/cargo/git

build:
	$(CARGO) build --release

test:
	$(CARGO) test --release --features testing

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
