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
 * Time-series chart panel showing CPU utilization percentage per worker.
 */
export const CpuChartPanel: React.FC = () => {
  const metrics = useAtomValue(metricsAtom);
  const zoomRange = useAtomValue(chartZoomRangeAtom);
  const setZoomRange = useSetAtom(chartZoomRangeAtom);
  const isStacked = useAtomValue(chartsStackedAtom);
  const chartRef = useRef<ChartJS<'line'>>(null);

  // Apply shared zoom range when it changes
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;

    if (zoomRange) {
      chart.options.scales!.x!.min = zoomRange.min;
      chart.options.scales!.x!.max = zoomRange.max;
    } else {
      chart.options.scales!.x!.min = undefined;
      chart.options.scales!.x!.max = undefined;
    }
    chart.update('none');
  }, [zoomRange]);

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

    const datasets = metrics.workers.map((worker, i) => ({
      label: worker.worker_id,
      data: worker.snapshots.map((s) => s.cpu_percent ?? 0),
      borderColor: CHART_COLORS[i % CHART_COLORS.length],
      backgroundColor: isStacked
        ? `${CHART_COLORS[i % CHART_COLORS.length]}80`
        : `${CHART_COLORS[i % CHART_COLORS.length]}20`,
      borderWidth: 1.5,
      pointRadius: 0,
      tension: 0.3,
      fill: isStacked,
    }));

    return { labels, datasets };
  }, [metrics, isStacked]);

  const handleZoomComplete = ({ chart }: { chart: ChartJS }) => {
    const xScale = chart.scales.x;
    if (xScale) {
      setZoomRange({ min: xScale.min, max: xScale.max });
    }
  };

  return (
    <div className="panel-container" style={{ display: 'flex', flexDirection: 'column' }}>
      <h2 className="panel-title">CPU Utilization (%)</h2>
      <div className="chart-container">
        {chartData.datasets.length > 0 ? (
          <Line
            ref={chartRef}
            data={chartData}
            options={{
              responsive: true,
              maintainAspectRatio: false,
              animation: { duration: 0 },
              interaction: {
                mode: isStacked ? 'index' : 'nearest',
                intersect: false,
              },
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
                  pan: {
                    enabled: true,
                    mode: 'x',
                    onPanComplete: handleZoomComplete,
                  },
                  zoom: {
                    drag: { enabled: true },
                    mode: 'x',
                    onZoomComplete: handleZoomComplete,
                  },
                },
              },
              scales: {
                x: {
                  ticks: { color: '#7d8590', maxTicksLimit: 8 },
                  grid: { color: '#21262d' },
                  min: zoomRange?.min,
                  max: zoomRange?.max,
                },
                y: {
                  stacked: isStacked,
                  ticks: { color: '#7d8590' },
                  grid: { color: '#21262d' },
                  min: 0,
                  max: isStacked ? undefined : 100,
                },
              },
            }}
          />
        ) : (
          <div className="empty-state">Waiting for metrics...</div>
        )}
      </div>
    </div>
  );
};

export default CpuChartPanel;
