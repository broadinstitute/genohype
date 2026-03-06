# Genohype Build Targets

# Default features for worker binary
WORKER_FEATURES ?= clickhouse

.PHONY: all release worker clean dashboard

# Default: build dashboard, then macOS CLI and Linux worker
all: dashboard release worker

# Build dashboard SPA and copy to cli/static/dist for embedding
dashboard:
	@echo "Building pool-dashboard SPA..."
	@cd frontend/pool-dashboard && npm run build
	@echo "Copying dist to cli/static/dist..."
	@rm -rf cli/static/dist
	@cp -r frontend/pool-dashboard/dist cli/static/dist
	@echo "Dashboard built and installed to cli/static/dist"

# Build macOS release binary (depends on dashboard for embedding)
release: dashboard
	cargo build --release

# Build Linux worker binary (cross-compile, depends on dashboard for embedding)
worker: dashboard
	@echo "Building Linux worker binary..."
	@ulimit -n 16384 2>/dev/null || ulimit -n 8192 2>/dev/null || true; \
	cargo zigbuild --target x86_64-unknown-linux-gnu --release --features $(WORKER_FEATURES)
	@mkdir -p target/release
	@cp target/x86_64-unknown-linux-gnu/release/genohype target/release/genohype-worker
	@echo "Installed: target/release/genohype-worker"

# Build both with all features
full: dashboard
	cargo build --release --features full
	@ulimit -n 16384 2>/dev/null || ulimit -n 8192 2>/dev/null || true; \
	cargo zigbuild --target x86_64-unknown-linux-gnu --release --features full
	@mkdir -p target/release
	@cp target/x86_64-unknown-linux-gnu/release/genohype target/release/genohype-worker
	@echo "Installed: target/release/genohype-worker"

clean:
	cargo clean
	rm -rf cli/static/dist
