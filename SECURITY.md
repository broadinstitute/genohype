# Security policy

## Reporting a vulnerability

Please report suspected vulnerabilities through [GitHub private vulnerability reporting](https://github.com/broadinstitute/genohype/security/advisories/new). Do not open a public issue with exploit details.

If private reporting is unavailable, contact a maintainer listed in [MAINTAINERS.md](MAINTAINERS.md) through an established private Broad Institute channel before sending technical details.

Include, when available:

- the affected Genohype version or commit;
- the affected command, crate, feature, or release asset;
- prerequisites and a minimal reproduction using synthetic data;
- the expected and observed behavior;
- the likely impact; and
- whether the issue may also affect an upstream dependency or downstream project.

Do not include real credentials, signed URLs, controlled-access data, individual-level genomic records, or other sensitive material. Redact logs and use synthetic fixtures.

## Supported versions

Genohype is pre-1.0 software. Security work targets the current `main` branch and the latest published release when a supported release is affected.

| Version | Security support |
|---|---|
| Current `main` | Yes |
| Latest published release | Yes, when affected and a fix is feasible |
| Older releases | No guaranteed support; upgrade to the latest release |

A correction to a published release will use a new version. Published tags and release assets will not be moved or silently replaced.

## Scope

Security reports may cover:

- parsing or decompression of untrusted genomic files;
- HTTP servers, MCP interfaces, and query endpoints;
- cloud credentials, instance metadata, signed URLs, and object-store access;
- distributed coordinator/worker communication;
- installer, checksum, archive, CI, and release-publishing behavior;
- dependency vulnerabilities that are reachable in supported builds; and
- unintended disclosure through logs, reports, caches, or generated artifacts.

A vulnerable development server or test-only dependency may have a different release impact than reachable runtime code, but it will still be assessed rather than dismissed solely from its package category.

## Genomic data and credential handling

Genohype is intended to process genomic summary statistics in its federation workflows. It does not make consent or institutional data-release decisions. Operators remain responsible for data classification, authorization, cloud configuration, retention, and audit requirements.

When reporting or testing a problem:

- use synthetic or explicitly public data;
- do not upload individual-level or controlled-access records;
- do not commit service-account keys, access tokens, cookies, signed URLs, bucket inventories, or internal hostnames;
- assume execution records and QC reports may be shared unless deployment policy says otherwise; and
- rotate any credential that may have been exposed.

## Response and disclosure

Maintainers will acknowledge and assess reports as capacity permits; this policy does not promise a fixed response or remediation time. The reporter and maintainers should coordinate public disclosure when practical, including any affected upstream or downstream projects.

Security fixes may be released before full technical detail is published. Credit will be offered unless the reporter requests anonymity or disclosure would create additional risk.
