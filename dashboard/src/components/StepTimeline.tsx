import type { Step } from "../types";
import { StatusBadge } from "./StatusBadge";

function formatTime(value?: string) {
  return value ? new Date(value).toLocaleTimeString() : "-";
}

function StepRow({ step }: { step: Step }) {
  return (
    <li className={`step-row step-${step.status.toLowerCase()}`}>
      <div className="step-row-main">
        <span className="step-name">{step.name}</span>
        <StatusBadge status={step.status} />
        <span className="step-attempt">
          attempt {step.attempt}/{step.max_attempts}
        </span>
      </div>
      <div className="step-row-meta">
        {step.started_at && <span>started {formatTime(step.started_at)}</span>}
        {step.lease_owner && step.status === "LEASED" && (
          <span>leased by {step.lease_owner}</span>
        )}
        {step.error && <span className="step-error">{step.error}</span>}
      </div>
    </li>
  );
}

export function StepTimeline({ steps }: { steps: Step[] }) {
  const forward = steps.filter((s) => !s.is_compensation).sort((a, b) => a.step_index - b.step_index);
  const compensations = steps
    .filter((s) => s.is_compensation)
    .sort((a, b) => a.step_index - b.step_index);

  return (
    <div className="step-timeline">
      <ol className="step-list">
        {forward.map((step) => (
          <StepRow key={step.id} step={step} />
        ))}
      </ol>
      {compensations.length > 0 && (
        <>
          <h4 className="compensation-heading">Compensation</h4>
          <ol className="step-list">
            {compensations.map((step) => (
              <StepRow key={step.id} step={step} />
            ))}
          </ol>
        </>
      )}
    </div>
  );
}
