import { useAtomValue } from 'jotai';
import { eventsAtom } from '../../atoms/dashboardAtoms';
import type { JobEvent } from '../../types';
import '../panels.css';

/**
 * Displays a scrolling terminal-like list of routine system events.
 * Subscribes to eventsAtom for historical cluster events.
 */
export const EventLogPanel: React.FC = () => {
  const events = useAtomValue(eventsAtom);

  return (
    <div className="panel-container">
      <h2 className="panel-title">System Events</h2>
      <div className="log-list">
        {events.length === 0 ? (
          <div className="empty-state" style={{ height: 'auto' }}>
            No events recorded yet...
          </div>
        ) : (
          // Show events in reverse chronological order (newest first)
          [...events].reverse().map((event, idx) => (
            <EventLogItem key={`${event.timestamp_ms}-${idx}`} event={event} />
          ))
        )}
      </div>
    </div>
  );
};

/**
 * Individual event log entry with styling based on event type.
 */
const EventLogItem: React.FC<{ event: JobEvent }> = ({ event }) => {
  const getEventClass = (eventType: string): string => {
    const type = eventType.toLowerCase();
    if (type === 'completed' || type === 'success') return 'success';
    if (type === 'failed' || type === 'error') return 'error';
    if (type === 'requeued' || type === 'warning') return 'warning';
    return 'info';
  };

  const getEventColor = (eventType: string): string => {
    const type = eventType.toLowerCase();
    if (type === 'completed' || type === 'success') return 'var(--green)';
    if (type === 'failed' || type === 'error') return 'var(--red)';
    if (type === 'requeued' || type === 'warning') return 'var(--yellow)';
    if (type === 'assigned') return 'var(--cyan)';
    return 'var(--accent)';
  };

  const formatTimestamp = (ms: number): string => {
    return new Date(ms).toLocaleTimeString('en-US', {
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
  };

  return (
    <div className={`log-item ${getEventClass(event.event_type)}`}>
      <span className="log-timestamp">[{formatTimestamp(event.timestamp_ms)}]</span>
      <span className="log-type" style={{ color: getEventColor(event.event_type) }}>
        {event.event_type.toUpperCase()}
      </span>
      {event.worker_id && (
        <span style={{ color: 'var(--cyan)', marginRight: '8px' }}>{event.worker_id}</span>
      )}
      {event.phenotype_id && (
        <span style={{ color: 'var(--purple)', marginRight: '8px' }}>
          {event.phenotype_id.split('/').pop()}
        </span>
      )}
      <span className="log-details">{event.details}</span>
    </div>
  );
};

export default EventLogPanel;
