# Genohype Build Targets

# Default features for worker binary
WORKER_FEATURES ?= clickhouse

.PHONY: all release worker clean

# Build both macOS CLI and Linux worker
all: release worker

# Build macOS release binary
release:
	cargo build --release

# Build Linux worker binary (cross-compile) and install to target/release
worker:
	@echo "Building Linux worker binary..."
	@ulimit -n 16384 2>/dev/null || ulimit -n 8192 2>/dev/null || true; \
	cargo zigbuild --target x86_64-unknown-linux-gnu --release --features $(WORKER_FEATURES)
	@mkdir -p target/release
	@cp target/x86_64-unknown-linux-gnu/release/genohype target/release/genohype-worker
	@echo "Installed: target/release/genohype-worker"

# Build both with specific features
full:
	cargo build --release --features full
	@ulimit -n 16384 2>/dev/null || ulimit -n 8192 2>/dev/null || true; \
	cargo zigbuild --target x86_64-unknown-linux-gnu --release --features full
	@mkdir -p target/release
	@cp target/x86_64-unknown-linux-gnu/release/genohype target/release/genohype-worker
	@echo "Installed: target/release/genohype-worker"

clean:
	cargo clean
