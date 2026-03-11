import { useState, useEffect } from 'react';
import '../panels.css';

interface JobConfig {
  job: {
    assets_json?: string;
    output_dir?: string;
    analysis_ids?: string[];
    ancestries?: string[];
    sample?: number;
    limit?: number;
    threshold: number;
    gene_threshold: number;
    locus_threshold: number;
    locus_window: number;
    locus_plots: boolean;
    min_variants_per_locus: number;
    width: number;
    height: number;
    y_field: string;
    scan_only: boolean;
    aggregate_only: boolean;
    genes?: string;
    exome_annotations?: string;
    genome_annotations?: string;
  };
  ingest: {
    input_dir?: string;
    clickhouse_url?: string;
    database: string;
    init_strategy: string;
  };
  styling: Record<string, unknown>;
}

const Row: React.FC<{ label: string; value: React.ReactNode }> = ({ label, value }) => (
  <tr>
    <td style={{ padding: '3px 12px 3px 0', color: 'var(--text-dim)', fontSize: '11px', whiteSpace: 'nowrap' }}>{label}</td>
    <td style={{ padding: '3px 0', fontSize: '11px', fontFamily: 'monospace' }}>{value}</td>
  </tr>
);

const Section: React.FC<{ title: string; children: React.ReactNode }> = ({ title, children }) => (
  <div style={{ marginBottom: '16px' }}>
    <h3 style={{ fontSize: '12px', fontWeight: 600, color: 'var(--cyan)', margin: '0 0 6px 0', textTransform: 'uppercase', letterSpacing: '0.5px' }}>{title}</h3>
    <table><tbody>{children}</tbody></table>
  </div>
);

export const JobConfigPanel: React.FC = () => {
  const [config, setConfig] = useState<JobConfig | null>(null);

  useEffect(() => {
    const fetchConfig = async () => {
      try {
        const res = await fetch('/api/catalog/config');
        if (res.ok) {
          const data = await res.json();
          if (data) setConfig(data);
        }
      } catch { /* ignore */ }
    };
    fetchConfig();
    const interval = setInterval(fetchConfig, 10000);
    return () => clearInterval(interval);
  }, []);

  if (!config) {
    return (
      <div className="panel-container">
        <h2 className="panel-title">Job Config</h2>
        <div className="empty-state" style={{ padding: '24px' }}>
          No config available. Submit a batch job with --config to populate.
        </div>
      </div>
    );
  }

  const { job, ingest } = config;

  return (
    <div className="panel-container" style={{ overflowY: 'auto' }}>
      <h2 className="panel-title" style={{ marginBottom: '12px' }}>Job Config</h2>

      <Section title="Input / Output">
        {job.assets_json && <Row label="Assets JSON" value={job.assets_json} />}
        {job.output_dir && <Row label="Output Dir" value={job.output_dir} />}
        {job.ancestries && job.ancestries.length > 0 && (
          <Row label="Ancestries" value={job.ancestries.join(', ')} />
        )}
        {job.analysis_ids && job.analysis_ids.length > 0 && (
          <Row label="Analysis IDs" value={`${job.analysis_ids.length} specified`} />
        )}
        {job.sample != null && <Row label="Sample" value={String(job.sample)} />}
        {job.limit != null && <Row label="Limit" value={String(job.limit)} />}
      </Section>

      <Section title="Thresholds">
        <Row label="Significance" value={job.threshold.toExponential(1)} />
        <Row label="Gene Burden" value={job.gene_threshold.toExponential(1)} />
        <Row label="Locus Inclusion" value={job.locus_threshold} />
        <Row label="Locus Window" value={`${(job.locus_window / 1_000_000).toFixed(1)} Mb`} />
        <Row label="Min Variants/Locus" value={job.min_variants_per_locus} />
        <Row label="Locus Plots" value={job.locus_plots ? 'Yes' : 'No'} />
      </Section>

      <Section title="Rendering">
        <Row label="Dimensions" value={`${job.width} x ${job.height}`} />
        <Row label="P-value Field" value={job.y_field} />
        <Row label="Mode" value={job.scan_only ? 'Scan Only' : job.aggregate_only ? 'Aggregate Only' : 'Full'} />
      </Section>

      <Section title="References">
        {job.genes && <Row label="Genes" value={job.genes} />}
        {job.exome_annotations && <Row label="Exome Annotations" value={job.exome_annotations} />}
        {job.genome_annotations && <Row label="Genome Annotations" value={job.genome_annotations} />}
      </Section>

      <Section title="Ingest">
        <Row label="ClickHouse" value={ingest.clickhouse_url || '--'} />
        <Row label="Database" value={ingest.database} />
        <Row label="Init Strategy" value={ingest.init_strategy} />
        {ingest.input_dir && <Row label="Input Dir" value={ingest.input_dir} />}
      </Section>
    </div>
  );
};

export default JobConfigPanel;
