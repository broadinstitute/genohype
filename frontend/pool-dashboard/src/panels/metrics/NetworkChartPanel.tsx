import { useMemo, useRef, useEffect } from 'react';
import { useAtomValue, useSetAtom } from 'jotai';
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
import { metricsAtom, chartZoomRangeAtom, chartsStackedAtom } from '../../atoms/dashboardAtoms';
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
  const zoomRange = useAtomValue(chartZoomRangeAtom);
  const setZoomRange = useSetAtom(chartZoomRangeAtom);
  const isStacked = useAtomValue(chartsStackedAtom);
  const rxChartRef = useRef<ChartJS<'line'>>(null);
  const txChartRef = useRef<ChartJS<'line'>>(null);

  // Apply shared zoom range when it changes
  useEffect(() => {
    for (const ref of [rxChartRef, txChartRef]) {
      const chart = ref.current;
      if (!chart) continue;
      if (zoomRange) {
        chart.options.scales!.x!.min = zoomRange.min;
        chart.options.scales!.x!.max = zoomRange.max;
      } else {
        chart.options.scales!.x!.min = undefined;
        chart.options.scales!.x!.max = undefined;
      }
      chart.update('none');
    }
  }, [zoomRange]);

  const { rxData, txData } = useMemo(() => {
    if (!metrics || !metrics.workers || metrics.workers.length === 0) {
      return { rxData: { labels: [] as string[], datasets: [] as any[] }, txData: { labels: [] as string[], datasets: [] as any[] } };
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

    const rxDatasets: any[] = [];
    const txDatasets: any[] = [];

    metrics.workers.forEach((worker, i) => {
      const color = CHART_COLORS[i % CHART_COLORS.length];
      const bgColor = isStacked ? `${color}80` : `${color}20`;

      rxDatasets.push({
        label: worker.worker_id,
        data: worker.snapshots.map((s) => (s.network_rx_bytes_sec ?? 0) / 1e6),
        borderColor: color,
        backgroundColor: bgColor,
        borderWidth: 1.5,
        pointRadius: 0,
        tension: 0.3,
        fill: isStacked,
      });

      txDatasets.push({
        label: worker.worker_id,
        data: worker.snapshots.map((s) => (s.network_tx_bytes_sec ?? 0) / 1e6),
        borderColor: color,
        backgroundColor: bgColor,
        borderWidth: 1.5,
        pointRadius: 0,
        tension: 0.3,
        fill: isStacked,
      });
    });

    return {
      rxData: { labels, datasets: rxDatasets },
      txData: { labels, datasets: txDatasets },
    };
  }, [metrics, isStacked]);

  const handleZoomComplete = ({ chart }: { chart: ChartJS }) => {
    const xScale = chart.scales.x;
    if (xScale) {
      setZoomRange({ min: xScale.min, max: xScale.max });
    }
  };

  const commonOptions: any = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    interaction: {
      mode: isStacked ? 'index' : 'nearest',
      intersect: false,
    },
    plugins: {
      legend: { display: false },
      zoom: {
        pan: { enabled: true, mode: 'x', onPanComplete: handleZoomComplete },
        zoom: { drag: { enabled: true }, mode: 'x', onZoomComplete: handleZoomComplete },
      },
    },
    scales: {
      x: {
        ticks: { color: '#7d8590', maxTicksLimit: 8, display: false },
        grid: { color: '#21262d' },
        min: zoomRange?.min,
        max: zoomRange?.max,
      },
      y: {
        stacked: isStacked,
        ticks: { color: '#7d8590', maxTicksLimit: 5 },
        grid: { color: '#21262d' },
        beginAtZero: true,
      },
    },
  };

  return (
    <div className="panel-container" style={{ display: 'flex', flexDirection: 'column' }}>
      <h2 className="panel-title">Network Bandwidth (MB/s)</h2>
      {rxData.datasets.length > 0 ? (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', flex: 1, minHeight: 0 }}>
          <div style={{ flex: 1, minHeight: 0, position: 'relative' }}>
            <Line
              ref={rxChartRef}
              data={rxData}
              options={{
                ...commonOptions,
                plugins: {
                  ...commonOptions.plugins,
                  title: { display: true, text: 'Receive (RX)', color: '#7d8590', font: { size: 10, weight: 'normal' as const }, padding: { top: 0, bottom: 4 } },
                },
              }}
            />
          </div>
          <div style={{ flex: 1, minHeight: 0, position: 'relative' }}>
            <Line
              ref={txChartRef}
              data={txData}
              options={{
                ...commonOptions,
                plugins: {
                  ...commonOptions.plugins,
                  title: { display: true, text: 'Transmit (TX)', color: '#7d8590', font: { size: 10, weight: 'normal' as const }, padding: { top: 0, bottom: 4 } },
                  legend: { display: true, position: 'bottom' as const, labels: { color: '#7d8590', font: { size: 10 }, boxWidth: 12 } },
                },
                scales: {
                  ...commonOptions.scales,
                  x: { ...commonOptions.scales.x, ticks: { ...commonOptions.scales.x.ticks, display: true } },
                },
              }}
            />
          </div>
        </div>
      ) : (
        <div className="empty-state">Waiting for metrics...</div>
      )}
    </div>
  );
};

export default NetworkChartPanel;
