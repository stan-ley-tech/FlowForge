import { useEffect, useState } from "react";
import { listRuns } from "../api";
import type { Run } from "../types";
import { StatusBadge } from "./StatusBadge";

const POLL_INTERVAL_MS = 3000;

export function RunList({ onSelect }: { onSelect: (runId: string) => void }) {
  const [runs, setRuns] = useState<Run[]>([]);
  const [workflowFilter, setWorkflowFilter] = useState("");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    const load = () => {
      listRuns(workflowFilter || undefined)
        .then((data) => {
          if (!cancelled) {
            setRuns(data ?? []);
            setError(null);
          }
        })
        .catch((err) => {
          if (!cancelled) setError(String(err.message ?? err));
        });
    };

    load();
    const timer = setInterval(load, POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [workflowFilter]);

  return (
    <div>
      <div className="toolbar">
        <input
          placeholder="filter by workflow name"
          value={workflowFilter}
          onChange={(e) => setWorkflowFilter(e.target.value)}
        />
      </div>

      {error && <div className="error-banner">{error}</div>}

      <table className="run-table">
        <thead>
          <tr>
            <th>Workflow</th>
            <th>Run ID</th>
            <th>Status</th>
            <th>Created</th>
            <th>Updated</th>
          </tr>
        </thead>
        <tbody>
          {runs.map((run) => (
            <tr key={run.id} onClick={() => onSelect(run.id)} className="clickable">
              <td>{run.workflow_name}</td>
              <td className="mono">{run.id.slice(0, 8)}</td>
              <td>
                <StatusBadge status={run.status} />
              </td>
              <td>{new Date(run.created_at).toLocaleString()}</td>
              <td>{new Date(run.updated_at).toLocaleString()}</td>
            </tr>
          ))}
          {runs.length === 0 && !error && (
            <tr>
              <td colSpan={5} className="empty">
                No runs yet.
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
