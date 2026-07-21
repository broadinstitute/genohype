# Contributing to Genohype

Thank you for helping improve Genohype. Contributions may include bug fixes, tests, documentation, performance work, new data backends, and carefully scoped feature proposals.

## Community standards

Participation in this project is governed by the [Broad Institute Contributor Covenant Code of Conduct](https://github.com/broadinstitute/.github/blob/main/CODE_OF_CONDUCT.md).

Report security-sensitive problems through the process in [SECURITY.md](SECURITY.md), not in a public issue.

## Before opening a pull request

- Search [existing issues](https://github.com/broadinstitute/genohype/issues) before opening a new one.
- For a user-visible feature, format change, public API change, or new cloud dependency, open an issue first so that scope and compatibility can be discussed.
- Keep dependency upgrades separate from feature and documentation changes.
- Never include credentials, signed URLs, controlled-access genomic data, or individual-level data in an issue, fixture, log, or pull request.

Small synthetic fixtures and excerpts from explicitly public datasets are welcome when their origin and expected behavior are documented.

## Development setup

The supported development path uses a stable Rust toolchain. Building the embedded pool dashboard also requires Node.js 20 and npm.

```bash
git clone https://github.com/broadinstitute/genohype.git
cd genohype
./scripts/build-dashboard.sh
cargo build --workspace --locked
```

The default build includes GCP and validation support. Optional integrations are controlled by Cargo features; see [README.md](README.md) for the current feature matrix.

## Tests and checks

Run the repository verification script before requesting review:

```bash
./scripts/verify.sh
```

It builds the embedded dashboard and runs the clean-checkout Rust gates used by CI. Useful narrower checks include:

```bash
cargo fmt --all --check
cargo test --workspace --locked
cargo test --workspace --all-features --locked
cargo check --workspace --all-features --locked
cargo build --release --locked
```

Tests that require cloud credentials or paid infrastructure must not be part of the ordinary local or pull-request path. Document an opt-in command and expected cost for any such test.

### Hail and VCF fixtures

Regression tests should prefer small, checked-in fixtures under `core/tests/fixtures` or another test-local fixture directory. Record how a generated fixture was produced, but do not require Hail, Spark, Python, or cloud access to run the normal Rust test suite.

### VEP and LOFTEE changes

VEP/LOFTEE support is experimental and resolves fastVEP from an immutable integration-fork revision. Changes to that pin or behavior should be isolated from unrelated work and include:

- a clean, annotation-enabled build;
- targeted input/output tests;
- the exact fastVEP revision;
- compatibility or concordance evidence appropriate to the change; and
- an accurate description of upstream submission or acceptance status.

Do not describe a fork change as accepted upstream unless the upstream maintainers have merged it.

## Pull request expectations

A pull request should:

1. explain the problem and the chosen scope;
2. separate existing behavior, new behavior, and known limitations;
3. include tests or explain why a test is not applicable;
4. update user or developer documentation when behavior changes;
5. identify generated files and how they were regenerated;
6. pass `./scripts/verify.sh`; and
7. avoid unrelated formatting, dependency, or generated-file churn.

Reviewers may request domain review for changes affecting genomic representation, annotation, quality control, standards conformance, security, or benchmark interpretation.

## Commit and review practice

Prefer small, coherent commits that can be reviewed independently. Maintainers may squash commits at merge, but the pull request history should still make authorship and substantive review clear.

The `main` branch is protected. Changes are merged through pull requests after required checks pass and review conversations are resolved.

## Licensing

By contributing, you agree that your contribution will be licensed under the repository's [MIT License](LICENSE).
