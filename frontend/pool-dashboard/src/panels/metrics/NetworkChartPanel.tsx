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

// Register Chart.js components
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
 * Time-series chart panel showing network bandwidth (MB/s) per worker.
 * Combines RX and TX into total bandwidth for simplicity.
 */
export const NetworkChartPanel: React.FC = () => {
  const metrics = useAtomValue(metricsAtom);
  const chartRef = useRef<ChartJS<'line'>>(null);

  const chartData = useMemo(() => {
    if (!metrics || !metrics.workers || metrics.workers.length === 0) {
      return { labels: [], datasets: [] };
    }

    // Extract timestamps from the first worker with data
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

    // Create separate datasets for RX and TX per worker
    const datasets: Array<{
      label: string;
      data: number[];
      borderColor: string;
      backgroundColor: string;
      borderWidth: number;
      pointRadius: number;
      tension: number;
      fill: boolean;
      borderDash?: number[];
    }> = [];

    metrics.workers.forEach((worker, i) => {
      const color = CHART_COLORS[i % CHART_COLORS.length];

      // RX dataset (solid line)
      datasets.push({
        label: `${worker.worker_id} RX`,
        data: worker.snapshots.map((s) => (s.network_rx_bytes_sec ?? 0) / 1e6),
        borderColor: color,
        backgroundColor: `${color}20`,
        borderWidth: 1.5,
        pointRadius: 0,
        tension: 0.3,
        fill: false,
      });

      // TX dataset (dashed line)
      datasets.push({
        label: `${worker.worker_id} TX`,
        data: worker.snapshots.map((s) => (s.network_tx_bytes_sec ?? 0) / 1e6),
        borderColor: color,
        backgroundColor: `${color}10`,
        borderWidth: 1.5,
        pointRadius: 0,
        tension: 0.3,
        fill: false,
        borderDash: [4, 4],
      });
    });

    return { labels, datasets };
  }, [metrics]);

  return (
    <div className="panel-container" style={{ display: 'flex', flexDirection: 'column' }}>
      <h2 className="panel-title">Network Bandwidth (MB/s)</h2>
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
                    labels: {
                      color: '#7d8590',
                      font: { size: 10 },
                      boxWidth: 12,
                    },
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

export default NetworkChartPanel;
