# Association test fixture

`single-variant.ht` is a small deterministic GRCh38 Hail table for association visualization tests. `single-variant.tsv` contains the same 12 logical rows in a conventional flat format for generalized-input CLI tests. The fixtures contain keyed rows on `chr1` and `chr4` with these fields:

- `locus`, `alleles`
- `Pvalue`, `Pvalue_log10`
- `BETA`, `SE`
- `AF_Allele2`, `AC_Allele2`

The rows deliberately include ordinary and genome-wide-significant p-values, tied p-values, a `Pvalue == 0` row with `Pvalue_log10 == 320`, finite invalid values (negative and greater than one), and missing optional values. Consumers must skip invalid p-values rather than treating them as hits. NaN and infinite cases are covered by Phase 1 normalization unit tests so this fixture's JSON query output remains standards-compliant.

Routine tests use these checked-in fixtures and require no cloud credentials. Regenerate the Hail table with `data/test/association/generate_fixture.py`; keep the TSV rows semantically identical when refreshing either fixture. To refresh a larger uncommitted real-data subset for manual testing, run `data/test/association/refresh-height-fixture.sh`; its default output is under `/tmp`.
