BEGIN IMMEDIATE;

CREATE TABLE IF NOT EXISTS association_datasets (
  dataset_id TEXT PRIMARY KEY CHECK(length(CAST(dataset_id AS BLOB)) = 36),
  source_fingerprint TEXT CHECK(
    source_fingerprint IS NULL
      OR length(CAST(source_fingerprint AS BLOB)) BETWEEN 71 AND 71
  ),
  created_at TEXT NOT NULL CHECK(length(CAST(created_at AS BLOB)) BETWEEN 1 AND 64),
  status TEXT NOT NULL CHECK(status IN ('building', 'complete')),
  CHECK(status = 'building' OR source_fingerprint IS NOT NULL)
) STRICT;

CREATE TABLE IF NOT EXISTS association_points (
  dataset_id TEXT NOT NULL,
  source_ordinal INTEGER NOT NULL CHECK(source_ordinal >= 0 AND source_ordinal <= 9007199254740991),
  contig TEXT NOT NULL CHECK(length(CAST(contig AS BLOB)) BETWEEN 1 AND 4096),
  contig_rank INTEGER NOT NULL CHECK(contig_rank BETWEEN 1 AND 65535),
  position INTEGER NOT NULL CHECK(position > 0 AND position <= 9007199254740991),
  ref TEXT CHECK(ref IS NULL OR length(CAST(ref AS BLOB)) <= 4096),
  alt TEXT CHECK(alt IS NULL OR length(CAST(alt AS BLOB)) <= 4096),
  pvalue REAL NOT NULL CHECK(pvalue >= 0.0 AND pvalue <= 1.0),
  neg_log10_p REAL NOT NULL CHECK(neg_log10_p >= 0.0 AND neg_log10_p <= 1.7976931348623157e308),
  beta REAL CHECK(beta IS NULL OR abs(beta) <= 1.7976931348623157e308),
  se REAL CHECK(se IS NULL OR abs(se) <= 1.7976931348623157e308),
  af REAL CHECK(af IS NULL OR (af >= 0.0 AND af <= 1.0)),
  ac REAL CHECK(ac IS NULL OR (ac >= 0.0 AND ac <= 1.7976931348623157e308)),
  gene TEXT CHECK(gene IS NULL OR length(CAST(gene AS BLOB)) <= 4096),
  consequence TEXT CHECK(consequence IS NULL OR length(CAST(consequence AS BLOB)) <= 4096),
  series TEXT CHECK(series IS NULL OR length(CAST(series AS BLOB)) <= 4096),
  CHECK(
    length(CAST(contig AS BLOB))
      + coalesce(length(CAST(ref AS BLOB)), 0)
      + coalesce(length(CAST(alt AS BLOB)), 0)
      + coalesce(length(CAST(gene AS BLOB)), 0)
      + coalesce(length(CAST(consequence AS BLOB)), 0)
      + coalesce(length(CAST(series AS BLOB)), 0)
    <= 16384
  ),
  PRIMARY KEY(dataset_id, source_ordinal),
  FOREIGN KEY(dataset_id) REFERENCES association_datasets(dataset_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX IF NOT EXISTS association_region_idx
  ON association_points(dataset_id, contig, position);
CREATE INDEX IF NOT EXISTS association_pvalue_idx
  ON association_points(dataset_id, pvalue);

CREATE TABLE IF NOT EXISTS top_hits (
  dataset_id TEXT NOT NULL,
  rank INTEGER NOT NULL CHECK(rank >= 1 AND rank <= 1000),
  source_ordinal INTEGER NOT NULL,
  PRIMARY KEY(dataset_id, rank),
  UNIQUE(dataset_id, source_ordinal),
  FOREIGN KEY(dataset_id, source_ordinal)
    REFERENCES association_points(dataset_id, source_ordinal) ON DELETE CASCADE
) STRICT;

CREATE TABLE IF NOT EXISTS loci (
  dataset_id TEXT NOT NULL,
  locus_id TEXT NOT NULL CHECK(length(CAST(locus_id AS BLOB)) BETWEEN 1 AND 256),
  contig TEXT NOT NULL CHECK(length(CAST(contig AS BLOB)) BETWEEN 1 AND 4096),
  start INTEGER NOT NULL CHECK(start > 0),
  stop INTEGER NOT NULL CHECK(stop >= start),
  lead_source_ordinal INTEGER,
  lead_pvalue REAL CHECK(lead_pvalue IS NULL OR (lead_pvalue >= 0.0 AND lead_pvalue <= 1.0)),
  PRIMARY KEY(dataset_id, locus_id),
  FOREIGN KEY(dataset_id) REFERENCES association_datasets(dataset_id) ON DELETE CASCADE,
  FOREIGN KEY(dataset_id, lead_source_ordinal)
    REFERENCES association_points(dataset_id, source_ordinal)
) STRICT;

PRAGMA user_version = 1;
COMMIT;
