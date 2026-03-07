import { useMemo, useRef } from 'react';
import { useAtomValue } from 'jotai';
import { Line } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip as ChartTooltip,
  Legend,
  Filler,
} from 'chart.js';
import zoomPlugin from 'chartjs-plugin-zoom';
import { metricsAtom } from '../../atoms/dashboardAtoms';
import '../panels.css';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  ChartTooltip,
  Legend,
  Filler,
  zoomPlugin
);

const CHART_COLORS = ['#39c5cf', '#3fb950', '#db6d28', '#f85149', '#a371f7', '#58a6ff'];

/**
 * Time-series chart panel showing dynamic batch size (AIMD) per worker.
 * Shows the stepped AIMD scale-up and memory-based scale-down over time.
 */
export const BatchSizeChartPanel: React.FC = () => {
  const metrics = useAtomValue(metricsAtom);
  const chartRef = useRef<ChartJS<'line'>>(null);

  const chartData = useMemo(() => {
    if (!metrics || !metrics.workers || metrics.workers.length === 0) {
      return { labels: [], datasets: [] };
    }

    const activeWorker = metrics.workers.find((w) => w.snapshots.length > 0);
    const labels = activeWorker
      ? activeWorker.snapshots.map((s) =>
          new Date(s.timestamp_ms).toLocaleTimeString('en-US', {
            hour12: false,
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
          })
        )
      : [];

    const datasets = metrics.workers.map((worker, i) => ({
      label: worker.worker_id,
      data: worker.snapshots.map((s) => s.current_batch_size ?? 0),
      borderColor: CHART_COLORS[i % CHART_COLORS.length],
      backgroundColor: `${CHART_COLORS[i % CHART_COLORS.length]}20`,
      borderWidth: 1.5,
      pointRadius: 0,
      tension: 0.1, // Less smoothing to show stepped AIMD changes
      stepped: true, // AIMD makes discrete steps, stepped charting looks best
      fill: false,
    }));

    return { labels, datasets };
  }, [metrics]);

  return (
    <div className="panel-container" style={{ display: 'flex', flexDirection: 'column' }}>
      <h2 className="panel-title">Batch Size (AIMD)</h2>
      <div className="chart-container" style={{ position: 'relative' }}>
        {chartData.datasets.length > 0 ? (
          <>
            <button
              onClick={() => chartRef.current?.resetZoom()}
              style={{
                position: 'absolute',
                top: 0,
                right: 0,
                fontSize: '10px',
                padding: '2px 6px',
                background: 'var(--surface-hover)',
                border: '1px solid var(--border)',
                borderRadius: '4px',
                color: 'var(--text-dim)',
                cursor: 'pointer',
                zIndex: 10,
              }}
            >
              Reset Zoom
            </button>
            <Line
              ref={chartRef}
              data={chartData}
              options={{
                responsive: true,
                maintainAspectRatio: false,
                animation: { duration: 0 },
                plugins: {
                  legend: {
                    display: true,
                    position: 'bottom',
                    labels: { color: '#7d8590', font: { size: 10 }, boxWidth: 12 },
                  },
                  zoom: {
                    pan: { enabled: true, mode: 'x' },
                    zoom: {
                      drag: { enabled: true },
                      mode: 'x',
                    },
                  },
                },
                scales: {
                  x: {
                    ticks: { color: '#7d8590', maxTicksLimit: 8 },
                    grid: { color: '#21262d' },
                  },
                  y: {
                    ticks: { color: '#7d8590' },
                    grid: { color: '#21262d' },
                    beginAtZero: true,
                  },
                },
              }}
            />
          </>
        ) : (
          <div className="empty-state">Waiting for metrics...</div>
        )}
      </div>
    </div>
  );
};

export default BatchSizeChartPanel;
