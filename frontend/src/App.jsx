import { useEffect, useMemo, useState } from "react";

const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";
const SUGGESTED_TICKERS = ["AAPL", "MSFT", "TSLA", "NVDA", "AMZN", "GOOGL", "META"];
const OBSERVABILITY_LINKS = [
  { label: "Langfuse", href: "https://cloud.langfuse.com", note: "Traces and reasoning logs" },
  { label: "Prometheus", href: "http://localhost:9090", note: "Metrics queries + targets" },
  { label: "Grafana", href: "http://localhost:3000", note: "Dashboards and panels" }
];

async function getJson(path) {
  const response = await fetch(`${API_BASE}${path}`);
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`${response.status} ${detail}`);
  }
  return response.json();
}

function sanitizeTicker(value) {
  return value.trim().toUpperCase();
}

function PriceChart({ items }) {
  if (!items.length) {
    return <p className="empty-text">No price snapshots yet.</p>;
  }

  const ordered = [...items].reverse();
  const points = ordered.map((item) => Number(item.price));
  const min = Math.min(...points);
  const max = Math.max(...points);
  const ySpan = max - min || 1;

  const coords = ordered.map((item, index) => {
    const x = (index / Math.max(ordered.length - 1, 1)) * 100;
    const y = 100 - ((Number(item.price) - min) / ySpan) * 100;
    return `${x},${y}`;
  });

  return (
    <div className="chart-wrap">
      <svg viewBox="0 0 100 100" preserveAspectRatio="none" className="chart-svg">
        <polyline points={coords.join(" ")} />
      </svg>
      <div className="chart-footer">
        <span>Low: {min.toFixed(2)}</span>
        <span>High: {max.toFixed(2)}</span>
      </div>
    </div>
  );
}

function NewsPagination({ page, hasNext, onPrev, onNext }) {
  return (
    <div className="pager">
      <button type="button" onClick={onPrev} disabled={page <= 1}>
        Prev
      </button>
      <span>Page {page}</span>
      <button type="button" onClick={onNext} disabled={!hasNext}>
        Next
      </button>
    </div>
  );
}

export default function App() {
  const [watchlist, setWatchlist] = useState([]);
  const [selectedTicker, setSelectedTicker] = useState("");
  const [customTicker, setCustomTicker] = useState("");
  const [prices, setPrices] = useState([]);
  const [news, setNews] = useState([]);
  const [newsPage, setNewsPage] = useState(1);
  const [newsHasNext, setNewsHasNext] = useState(false);
  const [latest, setLatest] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const selectedInWatchlist = useMemo(
    () => (selectedTicker ? watchlist.includes(selectedTicker) : false),
    [watchlist, selectedTicker]
  );

  async function refreshWatchlist() {
    const payload = await getJson("/watchlist");
    setWatchlist(payload.tickers || []);
    if (!selectedTicker && payload.tickers?.length) {
      setSelectedTicker(payload.tickers[0]);
    } else if (selectedTicker && !payload.tickers.includes(selectedTicker)) {
      setSelectedTicker(payload.tickers[0] || "");
    }
  }

  async function loadTickerDetail(ticker, page) {
    if (!ticker) {
      setPrices([]);
      setNews([]);
      setLatest(null);
      return;
    }
    const [pricePayload, newsPayload, latestPayload] = await Promise.all([
      getJson(`/prices/${ticker}?page=1&limit=100`),
      getJson(`/news/${ticker}?page=${page}&limit=8`),
      getJson(`/latest/${ticker}`)
    ]);
    setPrices(pricePayload.items || []);
    setNews(newsPayload.items || []);
    setNewsHasNext(Boolean(newsPayload.has_next));
    setLatest(latestPayload);
  }

  useEffect(() => {
    setBusy(true);
    setError("");
    refreshWatchlist()
      .catch((err) => setError(`Failed loading watchlist: ${err.message}`))
      .finally(() => setBusy(false));
  }, []);

  useEffect(() => {
    if (!selectedTicker) {
      return;
    }
    setBusy(true);
    setError("");
    loadTickerDetail(selectedTicker, newsPage)
      .catch((err) => setError(`Failed loading detail: ${err.message}`))
      .finally(() => setBusy(false));
  }, [selectedTicker, newsPage]);

  async function addTicker(ticker) {
    const cleanTicker = sanitizeTicker(ticker);
    if (!cleanTicker) {
      return;
    }
    setBusy(true);
    setError("");
    try {
      const response = await fetch(`${API_BASE}/watchlist`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ticker: cleanTicker })
      });
      if (!response.ok && response.status !== 409) {
        throw new Error(await response.text());
      }
      await refreshWatchlist();
      setSelectedTicker(cleanTicker);
      setNewsPage(1);
      setCustomTicker("");
    } catch (err) {
      setError(`Failed adding ticker: ${err.message}`);
    } finally {
      setBusy(false);
    }
  }

  async function removeTicker(ticker) {
    setBusy(true);
    setError("");
    try {
      const response = await fetch(`${API_BASE}/watchlist/${ticker}`, { method: "DELETE" });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      await refreshWatchlist();
      setNewsPage(1);
    } catch (err) {
      setError(`Failed removing ticker: ${err.message}`);
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="app-shell">
      <header className="hero">
        <p className="eyebrow">T8 Watchlist + Detail UI</p>
        <h1>Market Observability Console</h1>
        <p className="subtitle">
          Select tracked tickers, inspect price + news context, and review LLM-generated reasoning.
        </p>
      </header>

      {error ? <p className="error-banner">{error}</p> : null}

      <section className="panel observability-panel">
        <h2>Observability Links</h2>
        <ul className="obs-links">
          {OBSERVABILITY_LINKS.map((item) => (
            <li key={item.label}>
              <a href={item.href} target="_blank" rel="noreferrer">
                {item.label}
              </a>
              <span>{item.note}</span>
            </li>
          ))}
        </ul>
      </section>

      <section className="layout-grid">
        <aside className="panel watchlist-panel">
          <h2>Watchlist Selector</h2>
          <div className="suggestions">
            {SUGGESTED_TICKERS.map((ticker) => (
              <button
                key={ticker}
                type="button"
                className={selectedTicker === ticker ? "chip chip-selected" : "chip"}
                onClick={() => {
                  setSelectedTicker(ticker);
                  setNewsPage(1);
                }}
              >
                {ticker}
              </button>
            ))}
          </div>

          <form
            className="add-form"
            onSubmit={(event) => {
              event.preventDefault();
              addTicker(customTicker);
            }}
          >
            <input
              value={customTicker}
              onChange={(event) => setCustomTicker(event.target.value)}
              placeholder="Add ticker (e.g. NFLX)"
              maxLength={10}
            />
            <button type="submit" disabled={busy}>
              Add
            </button>
          </form>

          <ul className="watchlist-items">
            {watchlist.map((ticker) => (
              <li key={ticker}>
                <button
                  type="button"
                  className={selectedTicker === ticker ? "ticker-btn active" : "ticker-btn"}
                  onClick={() => {
                    setSelectedTicker(ticker);
                    setNewsPage(1);
                  }}
                >
                  {ticker}
                </button>
                <button
                  type="button"
                  className="remove-btn"
                  disabled={!selectedInWatchlist || busy}
                  onClick={() => removeTicker(ticker)}
                >
                  Remove
                </button>
              </li>
            ))}
          </ul>
        </aside>

        <section className="detail-column">
          <article className="panel">
            <h2>{selectedTicker ? `${selectedTicker} Price Trend` : "Select a ticker"}</h2>
            <PriceChart items={prices} />
          </article>

          <div className="split-panels">
            <article className="panel">
              <h2>Latest News</h2>
              <NewsPagination
                page={newsPage}
                hasNext={newsHasNext}
                onPrev={() => setNewsPage((prev) => Math.max(prev - 1, 1))}
                onNext={() => setNewsPage((prev) => prev + 1)}
              />
              <ul className="news-list">
                {news.map((item, index) => (
                  <li key={`${item.headline}-${index}`}>
                    <a href={item.url || "#"} target="_blank" rel="noreferrer">
                      {item.headline}
                    </a>
                    <span>
                      {item.source} | {item.published_at || item.fetched_at}
                    </span>
                  </li>
                ))}
                {!news.length ? <li className="empty-text">No news items available.</li> : null}
              </ul>
            </article>

            <article className="panel">
              <h2>LLM Explanation</h2>
              {latest && latest.status !== "pending" ? (
                <div className="analysis-block">
                  <p>{latest.summary}</p>
                  <p>
                    <strong>Hypothesis:</strong>{" "}
                    {latest.hypothesis || "No hypothesis available in this record."}
                  </p>
                  <p>
                    <strong>Sentiment:</strong> {latest.sentiment} |{" "}
                    <strong>Trigger:</strong> {latest.trigger_reason || "none"}
                  </p>
                </div>
              ) : (
                <p className="empty-text">No analysis generated yet for this ticker.</p>
              )}
            </article>
          </div>
        </section>
      </section>
    </main>
  );
}
