# FlowForge dashboard

A small React/TypeScript UI for watching runs against a FlowForge engine:
a list view polling `GET /v1/runs`, and a detail view showing each run's
steps and its full event history side by side.

```
npm install
npm run dev
```

Points at `http://localhost:8080` by default. Override with a `.env`
file:

```
VITE_FLOWFORGE_API_URL=http://localhost:9000
```

No build step is required to just look at data - the dashboard only
reads from the engine's REST API, it never writes to it.
