import { useEffect, useState } from "react";
import { getHistory, getRun } from "../api";
import type { HistoryEvent, RunDetail as RunDetailData } from "../types";
import { isTerminalRun } from "../types";
import { EventLog } from "./EventLog";
import { StatusBadge } from "./StatusBadge";
import { StepTimeline } from "./StepTimeline";

const POLL_INTERVAL_MS = 2000;

export function RunDetail({ runId, onBack }: { runId: string; onBack: () => void }) {
  const [run, setRun] = useState<RunDetailData | null>(null);
  const [events, setEvents] = useState<HistoryEvent[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout>;

    const load = () => {
      Promise.all([getRun(runId), getHistory(runId)])
        .then(([runData, historyData]) => {
          if (cancelled) return;
          setRun(runData);
          setEvents(historyData ?? []);
          setError(null);
          if (!isTerminalRun(runData.status)) {
            timer = setTimeout(load, POLL_INTERVAL_MS);
          }
        })
        .catch((err) => {
          if (!cancelled) setError(String(err.message ?? err));
        });
    };

    load();
    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [runId]);

  return (
    <div>
      <button className="back-link" onClick={onBack}>
        &larr; back to runs
      </button>

      {error && <div className="error-banner">{error}</div>}

      {run && (
        <>
          <div className="run-header">
            <h2>{run.workflow_name}</h2>
            <StatusBadge status={run.status} />
            <span className="mono">{run.id}</span>
          </div>
          {run.error && <div className="error-banner">{run.error}</div>}

          <div className="run-columns">
            <section>
              <h3>Steps</h3>
              <StepTimeline steps={run.steps} />
            </section>
            <section>
              <h3>History</h3>
              <EventLog events={events} />
            </section>
          </div>
        </>
      )}
    </div>
  );
}
