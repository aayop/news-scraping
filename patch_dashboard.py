from pathlib import Path

path = Path('dashboard.html')
text = path.read_text(encoding='utf-8')
start_marker = '// ── PASTE YOUR GOLD DATA HERE'
end_marker = '// ─── INIT'
start = text.index(start_marker)
end = text.index(end_marker)

replacement = '''// ─── LOAD GOLD DATA AUTOMATICALLY ─────────────────────────────────────────
//
// The dashboard now loads data from the built gold files at /data_lake/gold/*.json
// when served from a local server or the Docker dashboard service.

const DATA = {
  summary: {
    total_articles: 0,
    total_sources: 0,
    sources: [],
    languages: [],
    avg_content_length: 0,
    generated_at: null,
  },
  bySource: [],
  byLanguage: [],
  byDate: [],
  byCategory: [],
  keywords: [],
  articles: [],
};

async function fetchJson(path) {
  try {
    const res = await fetch(path);
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    return await res.json();
  } catch (error) {
    console.warn(`Dashboard: failed to load ${path}:`, error);
    return null;
  }
}

async function loadGoldData() {
  const [summary, bySource, byLanguage, byDate, byCategory, keywords] = await Promise.all([
    fetchJson('/data_lake/gold/summary_stats.json'),
    fetchJson('/data_lake/gold/articles_by_source.json'),
    fetchJson('/data_lake/gold/articles_by_language.json'),
    fetchJson('/data_lake/gold/articles_by_date.json'),
    fetchJson('/data_lake/gold/articles_by_category.json'),
    fetchJson('/data_lake/gold/top_keywords.json'),
  ]);

  if (summary) DATA.summary = summary;
  if (bySource) DATA.bySource = bySource;
  if (byLanguage) DATA.byLanguage = byLanguage;
  if (byDate) DATA.byDate = byDate;
  if (byCategory) DATA.byCategory = byCategory;
  if (keywords) DATA.keywords = keywords;
}

function showDataWarning() {
  if (DATA.bySource.length === 0 || DATA.byLanguage.length === 0) {
    const info = document.createElement('div');
    info.style.margin = '20px 0';
    info.style.padding = '16px';
    info.style.border = '1px solid rgba(255,255,255,0.12)';
    info.style.borderRadius = '14px';
    info.style.background = 'rgba(255,255,255,0.02)';
    info.style.color = 'var(--muted)';
    info.innerText = 'Real dashboard data could not be loaded automatically. Serve the project from the repository root with a local HTTP server or ensure /data_lake is available to the dashboard server.';
    document.querySelector('.container').insertBefore(info, document.querySelector('.stats-grid'));
  }
}

function renderStats() {
'''
new_text = text[:start] + replacement + text[end:]
path.write_text(new_text, encoding='utf-8')
print('dashboard.html updated')
