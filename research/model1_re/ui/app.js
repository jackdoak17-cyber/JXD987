const DEFAULT_URL = "/artifacts/model1_future_7d.json";

const $ = (id) => document.getElementById(id);

function parseNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function toDate(value) {
  // Input examples: "2026-04-18 17:00:00+00:00"
  if (!value) return null;
  // Normalize to ISO-ish: "2026-04-18T17:00:00+00:00"
  const iso = String(value).replace(" ", "T");
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

function fmtKickoff(value) {
  const d = toDate(value);
  if (!d) return "—";
  const date = new Intl.DateTimeFormat(undefined, { weekday: "short", month: "short", day: "2-digit" }).format(d);
  const time = new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit" }).format(d);
  return `${date} ${time}`;
}

function fmt(n, digits = 2) {
  if (n === null || n === undefined) return "—";
  const v = Number(n);
  if (!Number.isFinite(v)) return "—";
  return v.toFixed(digits);
}

function normalize(text) {
  return String(text || "").toLowerCase().trim();
}

function marketPill(marketType) {
  if (marketType === "shots") return `<span class="pill pill--shots">shots</span>`;
  if (marketType === "onTargetScoringAttempt") return `<span class="pill pill--sot">sot</span>`;
  return `<span class="pill">?</span>`;
}

function sortRows(rows, mode) {
  const copy = [...rows];
  switch (mode) {
    case "fixture_timestamp_asc":
      copy.sort((a, b) => (toDate(a.fixture_timestamp)?.getTime() || 0) - (toDate(b.fixture_timestamp)?.getTime() || 0));
      return copy;
    case "fixture_timestamp_desc":
      copy.sort((a, b) => (toDate(b.fixture_timestamp)?.getTime() || 0) - (toDate(a.fixture_timestamp)?.getTime() || 0));
      return copy;
    case "best_over_odds_desc":
      copy.sort((a, b) => (parseNumber(b.best_over_odds, 0) - parseNumber(a.best_over_odds, 0)));
      return copy;
    case "predicted_fair_over_odds_asc":
      copy.sort((a, b) => (parseNumber(a.predicted_fair_over_odds, 1e9) - parseNumber(b.predicted_fair_over_odds, 1e9)));
      return copy;
    case "predicted_over_edge_pct_desc":
    default:
      copy.sort((a, b) => (parseNumber(b.predicted_over_edge_pct, -1e9) - parseNumber(a.predicted_over_edge_pct, -1e9)));
      return copy;
  }
}

function applyFilters(rows) {
  const market = $("marketSelect").value;
  const minEdge = parseNumber($("minEdge").value, 0);
  const maxOdd = parseNumber($("maxOdd").value, 999);
  const q = normalize($("search").value);

  return rows.filter((r) => {
    if (market !== "all" && r.market_type !== market) return false;
    const edge = parseNumber(r.predicted_over_edge_pct, null);
    if (edge === null || edge < minEdge) return false;
    const odd = parseNumber(r.best_over_odds, null);
    if (odd === null || odd > maxOdd) return false;
    if (q) {
      const hay = normalize(`${r.player_name_db} ${r.home_team_name} ${r.away_team_name} ${r.market_type}`);
      if (!hay.includes(q)) return false;
    }
    return true;
  });
}

function setError(message) {
  const box = $("errorBox");
  if (!message) {
    box.classList.add("hidden");
    box.textContent = "";
    return;
  }
  box.classList.remove("hidden");
  box.textContent = message;
}

function renderSummary(allRows, shownRows) {
  $("rowsStat").textContent = String(allRows.length);
  $("shownStat").textContent = String(shownRows.length);

  let maxEdge = null;
  let lastKickoff = null;
  for (const r of shownRows) {
    const e = parseNumber(r.predicted_over_edge_pct, null);
    if (e !== null && (maxEdge === null || e > maxEdge)) maxEdge = e;
    const d = toDate(r.fixture_timestamp);
    if (d && (!lastKickoff || d > lastKickoff)) lastKickoff = d;
  }
  $("maxEdgeStat").textContent = maxEdge === null ? "—" : `${fmt(maxEdge, 1)}%`;
  $("lastKickoffStat").textContent = lastKickoff ? fmtKickoff(lastKickoff.toISOString()) : "—";
}

function renderTable(rows) {
  const tbody = $("betsTbody");
  tbody.innerHTML = "";
  const limit = parseNumber($("limitSelect").value, 100);
  const shown = limit === 0 ? rows : rows.slice(0, limit);

  const frag = document.createDocumentFragment();
  for (const r of shown) {
    const tr = document.createElement("tr");
    const edge = parseNumber(r.predicted_over_edge_pct, 0);
    const edgeClass = edge >= 20 ? "edge" : "edge edge--low";
    tr.innerHTML = `
      <td class="mono">${fmtKickoff(r.fixture_timestamp)}</td>
      <td>${r.home_team_name} <span class="muted">vs</span> ${r.away_team_name}</td>
      <td>${r.player_name_db}</td>
      <td class="mono muted">${r.player_position_mode_10 || "—"}</td>
      <td>${marketPill(r.market_type)}</td>
      <td class="num mono">${fmt(r.line, 1)}</td>
      <td class="num mono">${fmt(r.best_over_odds, 2)}</td>
      <td class="num mono">${fmt(r.predicted_fair_over_odds, 2)}</td>
      <td class="num mono ${edgeClass}">${fmt(edge, 1)}%</td>
      <td class="num mono">${fmt(r.predicted_mu, 3)}</td>
      <td class="num mono">${fmt(r.player_avg_60, 2)}</td>
      <td class="num mono">${fmt(r.player_per90_60, 2)}</td>
      <td class="num mono">${fmt((r.player_hit_rate_5 ?? null) * 100.0, 0)}%</td>
      <td class="num mono">${fmt((r.player_hit_rate_10 ?? null) * 100.0, 0)}%</td>
      <td class="num mono">${fmt((r.player_hit_rate_20 ?? null) * 100.0, 0)}%</td>
    `;
    frag.appendChild(tr);
  }
  tbody.appendChild(frag);
}

function rerender(state) {
  const filtered = applyFilters(state.rows);
  const sorted = sortRows(filtered, $("sortSelect").value);
  renderSummary(state.rows, sorted);
  renderTable(sorted);
}

async function loadFromUrl(url) {
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error(`Failed to fetch ${url} (${res.status})`);
  const rows = await res.json();
  if (!Array.isArray(rows)) throw new Error("JSON must be an array of rows.");
  return rows;
}

function loadFromFile(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      try {
        const rows = JSON.parse(String(reader.result || ""));
        if (!Array.isArray(rows)) throw new Error("JSON must be an array of rows.");
        resolve(rows);
      } catch (err) {
        reject(err);
      }
    };
    reader.onerror = () => reject(new Error("Failed to read file."));
    reader.readAsText(file);
  });
}

function attachHandlers(state) {
  const onChange = () => rerender(state);
  $("marketSelect").addEventListener("change", onChange);
  $("minEdge").addEventListener("input", onChange);
  $("maxOdd").addEventListener("input", onChange);
  $("search").addEventListener("input", onChange);
  $("sortSelect").addEventListener("change", onChange);
  $("limitSelect").addEventListener("change", onChange);

  $("reloadBtn").addEventListener("click", async () => {
    setError(null);
    try {
      $("sourceLabel").textContent = DEFAULT_URL;
      state.rows = await loadFromUrl(DEFAULT_URL);
      rerender(state);
    } catch (err) {
      setError(String(err?.message || err));
    }
  });

  $("fileInput").addEventListener("change", async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setError(null);
    try {
      $("sourceLabel").textContent = `file: ${file.name}`;
      state.rows = await loadFromFile(file);
      rerender(state);
    } catch (err) {
      setError(String(err?.message || err));
    }
  });
}

async function main() {
  const state = { rows: [] };
  attachHandlers(state);

  // Try the default URL first (works when served via http.server). If that fails,
  // user can still load a file manually.
  try {
    $("sourceLabel").textContent = DEFAULT_URL;
    state.rows = await loadFromUrl(DEFAULT_URL);
    rerender(state);
  } catch (err) {
    setError(
      `Could not load default JSON (${DEFAULT_URL}). If you're opening the file directly, start a local server: ` +
        `cd research/model1_re && python3 -m http.server 8000, then open http://localhost:8000/ui/. ` +
        `Or use “Load JSON…” to open a local file.`
    );
  }
}

main();
