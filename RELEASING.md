# Releasing Genohype

Genohype releases are built and published by [`.github/workflows/release.yml`](.github/workflows/release.yml). The workflow accepts stable `vX.Y.Z` tags, builds the embedded dashboard, compiles full-featured binaries, smoke-tests each binary, creates archives and `SHA256SUMS`, and publishes a GitHub release.

## Release contents

Each release contains:

- `genohype-vX.Y.Z-aarch64-apple-darwin.tar.gz`
- `genohype-vX.Y.Z-x86_64-apple-darwin.tar.gz`
- `genohype-vX.Y.Z-x86_64-unknown-linux-gnu.tar.gz`
- `genohype-worker-vX.Y.Z-x86_64-unknown-linux-gnu.tar.gz`
- `SHA256SUMS`

Only the `genohype` CLI is released. The Linux worker asset is the same full-featured CLI binary renamed for GCP pool deployment; `genohype-server` is not released separately.

## Prepare a release

1. Update the version in all four crate manifests: `cli/Cargo.toml`, `core/Cargo.toml`, `mcp/Cargo.toml`, and `pool/Cargo.toml`. Update `Cargo.lock` in the same commit.
2. Add `release-notes/vX.Y.Z.md`. State included functionality and known limitations directly.
3. Run the local gates:

   ```bash
   ./scripts/check-release-version.sh vX.Y.Z
   ./scripts/test-release.sh
   ./scripts/verify.sh
   ```

4. Merge the release-preparation pull request and wait for the `CI / verify` check on `main` to pass.
5. From the Actions page, run the Release workflow manually against `main`. A manual run builds and retains the complete release assets but does **not** publish a GitHub release. Inspect the `release-assets-vX.Y.Z` workflow artifact.

## Publish

Tag the exact tested commit on `main` and push the tag:

```bash
git switch main
git pull --ff-only origin main
git tag -a vX.Y.Z -m "Genohype vX.Y.Z"
git push origin vX.Y.Z
```

The tag-triggered workflow checks that the commit is on `main`, repeats the platform builds and smoke tests, and creates the GitHub release with the checked-in notes. The publishing job alone receives `contents: write`; all build jobs are read-only.

Do not move or replace a published tag. Correct a published release with a new patch version.

## Verify the published release

```bash
gh release view vX.Y.Z
gh release download vX.Y.Z --pattern SHA256SUMS

install_dir="$(mktemp -d)"
curl -fsSL https://raw.githubusercontent.com/broadinstitute/genohype/main/scripts/install.sh \
  | GENOHYPE_VERSION=vX.Y.Z GENOHYPE_INSTALL_DIR="$install_dir" sh
"$install_dir/genohype" --version
```

Test the default latest-release path once the explicit-version check succeeds:

```bash
curl -fsSL https://raw.githubusercontent.com/broadinstitute/genohype/main/scripts/install.sh | sh
genohype --version
```
