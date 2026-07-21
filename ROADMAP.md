# Genohype roadmap

This roadmap describes the intended direction of Genohype over approximately 24 months. It is a planning document, not a guarantee: priorities may change in response to correctness findings, security updates, user evidence, upstream changes, and maintainer capacity.

## Current baseline

Genohype v0.1.0 provides an installable, pre-1.0 command-line release for Apple Silicon macOS, Intel macOS, and x86-64 Linux. The current foundation includes:

- streaming access to Hail tables and indexed VCF files;
- local and cloud-backed querying and export;
- Parquet, VCF, Hail, ClickHouse, PostgreSQL, Elasticsearch, and BigQuery paths;
- visualization and reusable server components;
- distributed GCP worker pools and an embedded operator dashboard;
- reusable Rust crates consumed by project-specific genomic applications; and
- clean-checkout CI and tag-driven, checksummed binary releases.

The release contains optional VEP feature code, but VEP/LOFTEE annotation remains **experimental**. See [Experimental annotation status](#experimental-annotation-status).

## Status by capability

| Capability | Current status |
|---|---|
| Hail/VCF ingest, querying, export, and GCP pools | Available; derived from production use cases |
| Reusable Rust crates and server/MCP primitives | Available; pre-1.0 interfaces may change |
| Reproducible binary releases | Available for macOS and x86-64 Linux |
| Cross-backend correctness, performance, and cost evidence | Active development |
| Independent deployment and contribution documentation | Active development |
| Aggregate federation QC and reporting | Active development in the Browser Lite reference application |
| VEP/LOFTEE annotation | Experimental |
| VRS identity enforcement and limited Beacon access | Planned |
| AWS execution and Slurm adapters | Planned; object-storage access alone is not an execution adapter |

## Track 1: Evidence for portable genomic data serving

**Purpose:** make backend choices reproducible and evidence-based rather than anecdotal.

### Near term: 0–6 months

- Stabilize the multi-datastore workload and result-equivalence contract.
- Publish smoke-test inputs, commands, raw summaries, and explicit limitations.
- Separate database time, deserialization time, cache effects, and end-to-end latency.
- Record software revisions, infrastructure configuration, dataset identity, and cost assumptions with each result.

### 6–12 months

- Run chromosome 21 and 22 comparisons across at least five viable configurations.
- Publish per-query latency, throughput, failure, storage, and estimated cost results.
- Test cold, warm, and sustained-load behavior without presenting a single benchmark as a universal recommendation.

### 12–24 months

- Extend selected comparisons to larger representative workloads.
- Publish an architecture assessment that identifies which configurations fit different scales, operational constraints, and budgets.
- Convert reproducible findings into maintained deployment examples.

## Track 2: Independent deployment, contribution, and maintenance

**Purpose:** let groups install, evaluate, operate, and extend Genohype without relying on an unpublished local environment or direct maintainer intervention.

### Near term: 0–6 months

- Publish contribution, governance, security, support, citation, and roadmap documentation.
- Triage dependencies by runtime and release reachability; remediate security advisories in focused changes.
- Document the supported release platforms, pre-1.0 compatibility policy, and release process.
- Keep supported features buildable from a clean checkout against immutable public dependencies.

### 6–12 months

- Publish installation, cluster operation, end-to-end deployment, and federation guides.
- Publish provider-neutral Agent Skills with deterministic fixtures for recurring operator and extension tasks.
- Add a reviewed tutorial for implementing and testing a new QC check.
- Establish an explicit fastVEP fork, compatibility, and upstream-submission policy.

### 12–24 months

- Use evidence from external installations and contributions to revise setup, release, and maintenance practices.
- Evaluate additional packaging only where it improves a demonstrated installation path.
- Evaluate AWS execution, Slurm, Linux ARM64, notarization, and other distribution targets independently rather than promising them as one bundle.

## Track 3: Validated aggregate sharing

**Purpose:** provide an auditable path for institutions to validate and submit genomic summary statistics without transferring individual genotypes.

### Near term: 0–6 months

- Stabilize the streaming QC accumulator and versioned report schema.
- Keep technical-validity checks separate from biological-plausibility checks.
- Maintain deterministic clean and intentionally broken fixtures.

### 6–12 months

- Complete the `gbl qc` reference workflow in gnomAD Browser Lite.
- Implement and test at least 12 configured checks, including required fields, allele-count consistency, subgroup sums, missingness, site-frequency spectrum, transition/transversion, and sex-chromosome checks.
- Serve inspectable pass/warn/fail results through the report API, browser, and read-only MCP interface.

### 12–24 months

- Calibrate reference-dependent checks against named, versioned releases.
- Pilot local validation with participating institutions.
- Implement reviewed aggregate upload with execution records and an explicit boundary between software checks and institutional authorization.

Genohype handles summary statistics in this workflow. Sample- and genotype-level QC, consent interpretation, and decisions to share data remain with contributing institutions.

## Track 4: Canonical variant identity and standards-based access

**Purpose:** make equivalent variants match reliably across aggregate datasets and expose a deliberately limited standards-based discovery surface.

### Near term: 0–12 months

- Integrate versioned VRS and reference-sequence computation into the streaming path.
- Compare generated identifiers with the reference implementation on a predefined corpus.
- Record implementation, reference, and normalization versions in execution records.

### 12–24 months

- Enforce VRS identifier concordance during aggregate validation.
- Surface VRS identifiers through browser and read-only MCP responses.
- Implement and document a limited Beacon v2 `genomicVariations` profile for variant existence and aggregate frequency only.
- Run the relevant conformance tests and submit generally reusable fixes to upstream standards libraries.

## Experimental annotation status

Genohype v0.1.0 resolves its optional fastVEP crates from the immutable revision [`58f6c498be34b4b26a84a299195467c8b10fe155`](https://github.com/mattsolo1/fastVEP/commit/58f6c498be34b4b26a84a299195467c8b10fe155). That integration revision is public and reproducible, but it is not the current fastVEP upstream branch, the LOFTEE implementation has not been accepted upstream, and the broader integration stack has not yet completed the review and evidence required for a stable annotation claim.

Annotation will remain experimental until the consumed integration line is current and visibly tested, Genohype pins the reviewed result, and named-corpus concordance and performance evidence is public. Upstream submissions and their disposition will be described factually; submission is not acceptance, and no upstream collaboration will be implied without maintainer confirmation.

## Out of scope for Genohype

- storing or transferring individual-level genotypes as part of aggregate federation;
- reconstructing upstream sample- or genotype-level QC from aggregate counts;
- making institutional consent, authorization, or data-release decisions;
- replacing Hail, VEP, database engines, VRS, or Beacon; and
- treating one backend or deployment model as correct for every workload.

## Proposing changes

Open a [roadmap issue](https://github.com/broadinstitute/genohype/issues) with the affected track, user need, evidence, dependencies, and a bounded success criterion. See [CONTRIBUTING.md](CONTRIBUTING.md) for the contribution process and [MAINTAINERS.md](MAINTAINERS.md) for current project responsibility.
