import { useState } from "react";
import { RunDetail } from "./components/RunDetail";
import { RunList } from "./components/RunList";

export default function App() {
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);

  return (
    <div className="app">
      <header className="app-header">
        <h1>FlowForge</h1>
      </header>
      <main>
        {selectedRunId ? (
          <RunDetail runId={selectedRunId} onBack={() => setSelectedRunId(null)} />
        ) : (
          <RunList onSelect={setSelectedRunId} />
        )}
      </main>
    </div>
  );
}
