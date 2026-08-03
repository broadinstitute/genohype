#!/usr/bin/env bash
# Prepare the clean Ubuntu driver from host-built artifacts. No source/compiler is copied.
set -Eeuo pipefail

[[ $# -eq 2 ]] || {
  echo "usage: $0 <expected-full-revision> <driver-service-account>" >&2
  exit 2
}
EXPECTED_REVISION="$1"
DRIVER_SA="$2"

export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq apt-transport-https ca-certificates curl file gnupg jq python3 python3-venv

# The stock Ubuntu GCE image does not guarantee that gcloud is installed.
curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
  | sudo gpg --dearmor --yes -o /usr/share/keyrings/cloud.google.gpg
printf '%s\n' "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
  | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list >/dev/null
sudo apt-get update -qq
sudo apt-get install -y -qq google-cloud-cli

# Verify every transferred artifact before exercising the public installer path.
(
  cd /tmp/genohype-e2e-release
  sha256sum --check SHA256SUMS
)
GENOHYPE_VERSION=v0.0.0 \
GENOHYPE_RELEASE_BASE_URL=file:///tmp/genohype-e2e-release \
  sh /tmp/genohype-e2e-harness/install.sh
export PATH="$HOME/.local/bin:$PATH"

metadata_sa="$(curl -fsS -H 'Metadata-Flavor: Google' \
  http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email)"
[[ "$metadata_sa" == "$DRIVER_SA" ]] || {
  echo "driver metadata identity mismatch: $metadata_sa" >&2
  exit 1
}

genohype --version | tee /tmp/genohype-version.txt
short_revision="${EXPECTED_REVISION:0:7}"
grep -F "($short_revision)" /tmp/genohype-version.txt
command -v genohype-worker
test -x "$HOME/.local/bin/genohype"
test -e "$HOME/.local/bin/genohype-worker"

# Assert the guest has artifacts, not a checkout or compiler toolchain.
test ! -d /tmp/genohype-e2e-release/.git
if command -v cargo >/dev/null 2>&1 || command -v rustc >/dev/null 2>&1; then
  echo "unexpected Rust compiler toolchain on clean driver" >&2
  exit 1
fi

printf 'SETUP_OK revision=%s identity=%s\n' "$EXPECTED_REVISION" "$metadata_sa"
