CREATE TABLE IF NOT EXISTS gene_associations (
    -- Gene identity
    gene_id              String,
    gene_symbol          String,

    -- Test parameters
    annotation           LowCardinality(String),
    max_maf              Float64,

    -- Phenotype identity
    phenotype            String,
    ancestry             LowCardinality(String),

    -- Association stats (SKAT-O)
    pvalue               Nullable(Float64),
    pvalue_burden        Nullable(Float64),
    pvalue_skat          Nullable(Float64),
    beta_burden          Nullable(Float64),
    mac                  Nullable(Int64),
    mac_case             Nullable(Int64),
    mac_control          Nullable(Int64),

    -- Coordinates (for gene manhattan)
    contig               LowCardinality(String),
    gene_start_position  Int32,
    xpos                 Int64
)
ENGINE = MergeTree()
PARTITION BY phenotype
ORDER BY (ancestry, gene_id, annotation, max_maf)
SETTINGS index_granularity = 8192
