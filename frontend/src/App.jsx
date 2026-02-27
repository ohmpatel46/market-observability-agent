const surfaces = [
  { label: "API", href: "http://localhost:8000/health", note: "FastAPI service" },
  { label: "Prometheus", href: "http://localhost:9090", note: "Metrics backend" },
  { label: "Grafana", href: "http://localhost:3000", note: "Dashboards" }
];

const upcoming = [
  "Watchlist selector and ticker management UI",
  "Ticker detail page: price chart + paginated news",
  "Gemini-generated explanation panel tied to movement/news"
];

export default function App() {
  return (
    <main className="app-shell">
      <section className="hero">
        <p className="eyebrow">T5 Frontend Scaffold</p>
        <h1>Market Observability Console</h1>
        <p className="subtitle">
          React + Vite frontend is running in its own container and ready for data + watchlist
          workflows.
        </p>
      </section>

      <section className="panel-grid">
        <article className="panel">
          <h2>Service Access</h2>
          <ul className="surface-list">
            {surfaces.map((surface) => (
              <li key={surface.label}>
                <a href={surface.href} target="_blank" rel="noreferrer">
                  {surface.label}
                </a>
                <span>{surface.note}</span>
              </li>
            ))}
          </ul>
        </article>

        <article className="panel">
          <h2>Next UI Milestones</h2>
          <ul className="milestone-list">
            {upcoming.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </article>
      </section>
    </main>
  );
}
