const TONE: Record<string, string> = {
  QUEUED: "neutral",
  PENDING: "neutral",
  RUNNING: "active",
  READY: "active",
  LEASED: "active",
  COMPLETED: "success",
  COMPENSATED: "success",
  FAILED: "danger",
  CANCELLED: "danger",
  CANCELLING: "warning",
  COMPENSATING: "warning",
};

export function StatusBadge({ status }: { status: string }) {
  const tone = TONE[status] ?? "neutral";
  return <span className={`badge badge-${tone}`}>{status}</span>;
}
