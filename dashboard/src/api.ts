import type { HistoryEvent, Run, RunDetail } from "./types";

const BASE_URL = import.meta.env.VITE_FLOWFORGE_API_URL ?? "http://localhost:8080";

async function request<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.error ?? `request to ${path} failed with status ${res.status}`);
  }
  return res.json() as Promise<T>;
}

export function listRuns(workflow?: string): Promise<Run[]> {
  const query = workflow ? `?workflow=${encodeURIComponent(workflow)}` : "";
  return request<Run[]>(`/v1/runs${query}`);
}

export function getRun(id: string): Promise<RunDetail> {
  return request<RunDetail>(`/v1/runs/${id}`);
}

export function getHistory(id: string): Promise<HistoryEvent[]> {
  return request<HistoryEvent[]>(`/v1/runs/${id}/history`);
}
