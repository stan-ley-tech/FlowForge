import type { HistoryEvent } from "../types";

function formatDetail(detail: unknown): string {
  if (detail == null) return "";
  const text = JSON.stringify(detail);
  return text === "{}" ? "" : text;
}

export function EventLog({ events }: { events: HistoryEvent[] }) {
  return (
    <ol className="event-log">
      {events.map((event) => (
        <li key={event.id} className="event-row">
          <span className="event-time">{new Date(event.created_at).toLocaleTimeString()}</span>
          <span className="event-type">{event.type}</span>
          {event.step_name && <span className="event-step">{event.step_name}</span>}
          <span className="event-detail">{formatDetail(event.detail)}</span>
        </li>
      ))}
      {events.length === 0 && <li className="empty">No events yet.</li>}
    </ol>
  );
}
