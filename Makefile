# Genohype Build Targets

# Default features for worker binary
WORKER_FEATURES ?= clickhouse

.PHONY: all release worker clean dashboard association-viewer embedded-ui

# Default: build embedded UIs, then macOS CLI and Linux worker
all: embedded-ui release worker

# Build dashboard SPA and copy to cli/static/dist for embedding
dashboard:
	@./scripts/build-dashboard.sh

# Build the token-relative association viewer for embedding
association-viewer:
	@./scripts/build-association-viewer.sh

embedded-ui: dashboard association-viewer

# Build macOS release binary (depends on both embedded UIs)
release: embedded-ui
	cargo build --release

# Build Linux worker binary (cross-compile, depends on dashboard for embedding)
worker: embedded-ui
	@echo "Building Linux worker binary..."
	@ulimit -n 16384 2>/dev/null || ulimit -n 8192 2>/dev/null || true; \
	cargo zigbuild --target x86_64-unknown-linux-gnu --release --features $(WORKER_FEATURES)
	@mkdir -p target/release
	@cp target/x86_64-unknown-linux-gnu/release/genohype target/release/genohype-worker
	@echo "Installed: target/release/genohype-worker"

# Build both with all features
full: embedded-ui
	cargo build --release --features full
	@ulimit -n 16384 2>/dev/null || ulimit -n 8192 2>/dev/null || true; \
	cargo zigbuild --target x86_64-unknown-linux-gnu --release --features full
	@mkdir -p target/release
	@cp target/x86_64-unknown-linux-gnu/release/genohype target/release/genohype-worker
	@echo "Installed: target/release/genohype-worker"

clean:
	cargo clean
	rm -rf cli/static/dist cli/static/association-viewer/dist
