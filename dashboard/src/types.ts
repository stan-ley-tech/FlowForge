export type RunStatus =
  | "QUEUED"
  | "RUNNING"
  | "COMPLETED"
  | "FAILED"
  | "CANCELLING"
  | "CANCELLED"
  | "COMPENSATING"
  | "COMPENSATED";

export type StepStatus =
  | "PENDING"
  | "READY"
  | "LEASED"
  | "COMPLETED"
  | "FAILED"
  | "CANCELLED"
  | "COMPENSATED";

export interface Run {
  id: string;
  workflow_name: string;
  workflow_version: number;
  status: RunStatus;
  input: unknown;
  context: unknown;
  error?: string;
  created_at: string;
  updated_at: string;
}

export interface Step {
  id: string;
  run_id: string;
  name: string;
  step_index: number;
  is_compensation: boolean;
  compensation_of?: string;
  delay_seconds: number;
  status: StepStatus;
  attempt: number;
  max_attempts: number;
  timeout_seconds: number;
  scheduled_at: string;
  lease_owner?: string;
  lease_expires_at?: string;
  started_at?: string;
  result?: unknown;
  error?: string;
  idempotency_key: string;
  created_at: string;
  updated_at: string;
}

export interface RunDetail extends Run {
  steps: Step[];
}

export interface HistoryEvent {
  id: number;
  run_id: string;
  seq: number;
  type: string;
  step_name?: string;
  detail?: unknown;
  created_at: string;
}

const TERMINAL_RUN_STATUSES: ReadonlySet<RunStatus> = new Set([
  "COMPLETED",
  "FAILED",
  "CANCELLED",
  "COMPENSATED",
]);

export function isTerminalRun(status: RunStatus): boolean {
  return TERMINAL_RUN_STATUSES.has(status);
}
