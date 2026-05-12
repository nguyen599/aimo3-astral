/* ── AstralMath-v1 Dataset Viewer ─────────────────────────────── */

// ── Theme toggle ─────────────────────────────────────────────────
(function initTheme() {
  const saved = localStorage.getItem('theme');
  if (saved === 'dark') {
    document.documentElement.setAttribute('data-theme', 'dark');
  }
  // Light is default (no attribute needed)
})();

document.getElementById('theme-toggle').addEventListener('click', () => {
  const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
  if (isDark) {
    document.documentElement.removeAttribute('data-theme');
    localStorage.setItem('theme', 'light');
    document.getElementById('theme-toggle').innerHTML = '&#9790;'; // ☾
  } else {
    document.documentElement.setAttribute('data-theme', 'dark');
    localStorage.setItem('theme', 'dark');
    document.getElementById('theme-toggle').innerHTML = '&#9728;'; // ☀
  }
});

// Update icon on load
if (document.documentElement.getAttribute('data-theme') === 'dark') {
  document.getElementById('theme-toggle').innerHTML = '&#9728;';
}

// ── Config ───────────────────────────────────────────────────────
const HF_DATASET = 'nguyen599/AstralMath-v1';
const HF_DATASET_ERROR = 'nguyen599/AstralMath-v1-ErrorTraces';
const HF_DATASET_PREVIEW = 'nguyen599/AstralMath-v1-test';
const HF_DATASET_TFAIL = 'nguyen599/AstralMath-v1-consensus-failed';
const HF_API = 'https://datasets-server.huggingface.co';
const BATCH_SIZE = 20;  // rows to prefetch per batch
// Colors for grouping verifier solutions by extracted answer
const ANSWER_COLORS = ['#16a34a','#2563eb','#d97706','#db2777','#7c3aed','#ea580c','#0891b2','#dc2626'];

// ── Preview tab visibility flag ──────────────────────────────────
// Set to false to hide the Preview tab once judge scores are
// merged into the main Stage 1 / Stage 2 datasets.
const SHOW_PREVIEW_TAB = true;

function getPrefix(key) {
  return { stage1: 's1', stage2: 's2', preview: 'pv', bench: 'b', error: 'et', tfail: 'tf' }[key] || key;
}
function getHFDataset(key) {
  if (key === 'error') return HF_DATASET_ERROR;
  if (key === 'preview') return HF_DATASET_PREVIEW;
  if (key === 'tfail') return HF_DATASET_TFAIL;
  return HF_DATASET;
}

// HF datasets-server uses config+split. We try multiple patterns
// since HF auto-generates these from filenames in different ways.
const HF_SPLITS = {
  stage1: [
    { config: 'default', split: 'stage1' },
  ],
  stage2: [
    { config: 'default', split: 'stage2' },
  ],
  preview: [
    { config: 'default', split: 'train' },
  ],
  error: [
    { config: 'default', split: 'train' },
  ],
  tfail: [
    { config: 'default', split: 'train' },
  ],
};

// ── State ────────────────────────────────────────────────────────
const state = {
  stage1:  { cache: new Map(), idx: 0, total: 0, hfConfig: null, hfSplit: null, localData: null, loading: false },
  stage2:  { cache: new Map(), idx: 0, total: 0, hfConfig: null, hfSplit: null, localData: null, loading: false, showJudge: false },
  preview: { cache: new Map(), idx: 0, total: 0, hfConfig: null, hfSplit: null, localData: null, loading: false, showJudge: true },
  bench:   { cache: new Map(), idx: 0, total: 0, localData: null, loading: false },
  error:   { cache: new Map(), idx: 0, total: 0, hfConfig: null, hfSplit: null, localData: null, loading: false },
  tfail:   { cache: new Map(), idx: 0, total: 0, hfConfig: null, hfSplit: null, localData: null, loading: false, filterCategory: '', _filtered: null },
};

const KNOWN_MODELS = [
  'zai-org/GLM-5',
  'moonshotai/Kimi-K2.5',
  'Qwen/Qwen3.5-397B-A17B',
  'deepseek-ai/DeepSeek-V3.2',
  'stepfun-ai/Step-3.5-Flash',
  'openai/gpt-oss-120b',
  'nvidia/NVIDIA-Nemotron-3-Super-120B-A12B-BF16'
];

const statsTracker = {
  stage1:  { models: Object.fromEntries(KNOWN_MODELS.map(m => [m, 0])), sources: {} },
  stage2:  { models: Object.fromEntries(KNOWN_MODELS.map(m => [m, 0])), sources: {} },
  preview: { models: Object.fromEntries(KNOWN_MODELS.map(m => [m, 0])), sources: {} },
  error:   { models: Object.fromEntries(KNOWN_MODELS.map(m => [m, 0])), sources: {} },
};

// ── Routing ──────────────────────────────────────────────────────
function navigate() {
  const raw = location.hash.replace('#', '') || 'overview';
  // Support shareable URLs like #stage1/42
  const parts = raw.split('/');
  const page = ['overview', 'stage1', 'stage2', 'preview', 'bench', 'error', 'report', 'tfail'].includes(parts[0]) ? parts[0] : 'overview';
  const urlIdx = parts[1] ? parseInt(parts[1], 10) - 1 : null;

  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));

  document.getElementById('page-' + page).classList.add('active');
  const link = document.querySelector(`.nav-link[data-page="${page}"]`);
  if (link) link.classList.add('active');

  // Auto-init HF streaming when navigating to a stage/error/preview page
  const hfPages = ['stage1', 'stage2', 'preview', 'error'];
  if (hfPages.includes(page) && !state[page].localData && !state[page].hfSplit) {
    initHFStream(page).then(() => {
      if (urlIdx !== null && urlIdx >= 0 && urlIdx < state[page].total) {
        goToIndex(page, urlIdx);
      }
    });
  } else if (hfPages.includes(page) && urlIdx !== null && urlIdx >= 0) {
    goToIndex(page, urlIdx);
  }

  // Auto-load bench CSV when navigating to bench page
  if (page === 'bench' && !state.bench.localData && !state.bench.loading) {
    loadBenchCSV().then(() => {
      if (urlIdx !== null && urlIdx >= 0 && urlIdx < state.bench.total) {
        goToIndex('bench', urlIdx);
      }
    });
  } else if (page === 'bench' && urlIdx !== null && urlIdx >= 0) {
    goToIndex('bench', urlIdx);
  }

  // Auto-load transform failures from HuggingFace when navigating to tfail page
  if (page === 'tfail' && !state.tfail.localData && !state.tfail.hfSplit && !state.tfail.loading) {
    initHFStream('tfail').then(() => {
      if (urlIdx !== null && urlIdx >= 0 && urlIdx < state.tfail.total) {
        goToIndex('tfail', urlIdx);
      }
    });
  } else if (page === 'tfail' && (state.tfail.localData || state.tfail.hfSplit) && urlIdx !== null && urlIdx >= 0) {
    goToIndex('tfail', urlIdx);
  }

  // Render math on overview page
  if (page === 'overview') {
    requestAnimationFrame(() => {
      const el = document.getElementById('page-overview');
      if (el && typeof renderMathInElement === 'function') {
        renderMathInElement(el, {
          delimiters: [
            { left: '$$', right: '$$', display: true },
            { left: '$', right: '$', display: false },
          ],
          throwOnError: false,
        });
      }
    });
  }
}
window.addEventListener('hashchange', navigate);
navigate();

// ── HuggingFace Datasets Server API ─────────────────────────────
async function hfFetchRows(config, split, offset, length, dataset = HF_DATASET) {
  const url = `${HF_API}/rows?dataset=${encodeURIComponent(dataset)}&config=${encodeURIComponent(config)}&split=${encodeURIComponent(split)}&offset=${offset}&length=${length}`;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`HF API ${resp.status}`);
  return resp.json();
}

async function initHFStream(key) {
  const s = state[key];
  if (s.hfSplit || s.localData || s.loading) return;
  s.loading = true;

  const prefix = getPrefix(key);
  const card = document.getElementById(`${prefix}-card`);
  showCardLoading(card, `Connecting to HuggingFace...`);

  const candidates = HF_SPLITS[key] || [];
  let found = false;

  for (const { config, split } of candidates) {
    try {
      const result = await hfFetchRows(config, split, 0, 1, getHFDataset(key));
      if (result && result.num_rows_total > 0) {
        s.hfConfig = config;
        s.hfSplit = split;
        s.total = result.num_rows_total;
        // Cache first row
        if (result.rows && result.rows.length > 0) {
          s.cache.set(0, result.rows[0].row);
        }
        found = true;
        break;
      }
    } catch (e) {
      // Try next candidate
    }
  }

  s.loading = false;

  if (found) {
    const totalEl = document.getElementById(`${prefix}-total`);
    if (totalEl) totalEl.textContent = s.total.toLocaleString();
    await fetchAndRender(key, 0);
  } else {
    showCardUpload(card, prefix);
  }
}

async function fetchAndRender(key, idx) {
  const s = state[key];
  if (idx < 0 || idx >= s.total) return;
  s.idx = idx;

  const prefix = getPrefix(key);

  // If local data loaded (from file upload)
  if (s.localData) {
    renderItemFromData(key, s.localData[idx]);
    updateControls(key);
    return;
  }

  // Check cache
  if (s.cache.has(idx)) {
    renderItemFromData(key, s.cache.get(idx));
    updateControls(key);
    // Prefetch next batch in background
    prefetchBatch(key, idx + 1);
    return;
  }

  // Fetch from HF
  const card = document.getElementById(`${prefix}-card`);
  showCardLoading(card, `Loading row ${idx + 1}...`);

  try {
    const batchStart = idx;
    const batchLen = Math.min(BATCH_SIZE, s.total - batchStart);
    const result = await hfFetchRows(s.hfConfig, s.hfSplit, batchStart, batchLen, getHFDataset(key));

    if (result.rows) {
      for (let i = 0; i < result.rows.length; i++) {
        const row = result.rows[i].row;
        s.cache.set(batchStart + i, row);
        // Track stats for filter population
        if (key !== 'bench' && statsTracker[key]) {
          const t = statsTracker[key];
          if (row.model) t.models[row.model] = (t.models[row.model] || 0) + 1;
          if (row.source) t.sources[row.source] = (t.sources[row.source] || 0) + 1;
        }
      }
      if (key !== 'bench' && key !== 'tfail') {
        updateFilterOptions(key);
        updateMiniStats(key);
      }
    }

    if (s.cache.has(idx)) {
      renderItemFromData(key, s.cache.get(idx));
    }
  } catch (e) {
    card.innerHTML = `<div class="card-placeholder"><p style="color:#f85149">Failed to load: ${escapeHtml(e.message)}</p></div>`;
  }

  updateControls(key);
}

function prefetchBatch(key, startIdx) {
  const s = state[key];
  if (!s.hfSplit || startIdx >= s.total) return;
  // Don't prefetch if already cached
  if (s.cache.has(startIdx)) return;

  const batchLen = Math.min(BATCH_SIZE, s.total - startIdx);
  hfFetchRows(s.hfConfig, s.hfSplit, startIdx, batchLen, getHFDataset(key))
    .then(result => {
      if (result.rows) {
        for (let i = 0; i < result.rows.length; i++) {
          const row = result.rows[i].row;
          s.cache.set(startIdx + i, row);
          // Track stats for filter population
          if (key !== 'bench' && statsTracker[key]) {
            const t = statsTracker[key];
            if (row.model) t.models[row.model] = (t.models[row.model] || 0) + 1;
            if (row.source) t.sources[row.source] = (t.sources[row.source] || 0) + 1;
          }
        }
        if (key !== 'bench' && key !== 'tfail') {
          updateFilterOptions(key);
          updateMiniStats(key);
        }
      }
    })
    .catch(() => { /* silent prefetch failure */ });
}

// ── Card helpers ─────────────────────────────────────────────────
function showCardLoading(card, text) {
  card.innerHTML = `<div class="card-placeholder"><div class="loading-spinner">${escapeHtml(text)}</div></div>`;
}

function showCardUpload(card, prefix) {
  card.innerHTML = `
    <div class="card-placeholder">
      <p>Could not connect to HuggingFace. Upload file manually:</p>
      <label class="btn btn-primary file-label">
        Choose JSONL file
        <input type="file" id="${prefix}-file-fallback" accept=".jsonl" hidden>
      </label>
      <p class="hint">Or drop a file anywhere on this page</p>
    </div>`;
  const input = document.getElementById(`${prefix}-file-fallback`);
  if (input) {
    input.addEventListener('change', (e) => {
      const keyMap = { s1: 'stage1', s2: 'stage2', et: 'error', tf: 'tfail', pv: 'preview' };
      readFile(e.target.files[0], keyMap[prefix] || 'stage1', 'jsonl');
    });
  }
}

// ── File Loading (local fallback) ────────────────────────────────
function parseJSONL(text) {
  const lines = text.trim().split('\n');
  const data = [];
  for (const line of lines) {
    if (line.trim()) {
      try { data.push(JSON.parse(line)); } catch (e) { /* skip bad lines */ }
    }
  }
  return data;
}

function parseCSV(text) {
  // Full RFC 4180 parser: handles newlines, commas, and quotes inside quoted fields
  const rows = [];
  let row = [];
  let field = '';
  let inQuotes = false;
  let i = 0;

  while (i < text.length) {
    const ch = text[i];

    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < text.length && text[i + 1] === '"') {
          field += '"';
          i += 2;
        } else {
          inQuotes = false;
          i++;
        }
      } else {
        field += ch;
        i++;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
        i++;
      } else if (ch === ',') {
        row.push(field);
        field = '';
        i++;
      } else if (ch === '\r' && i + 1 < text.length && text[i + 1] === '\n') {
        row.push(field);
        field = '';
        rows.push(row);
        row = [];
        i += 2;
      } else if (ch === '\n') {
        row.push(field);
        field = '';
        rows.push(row);
        row = [];
        i++;
      } else {
        field += ch;
        i++;
      }
    }
  }
  if (field || row.length > 0) {
    row.push(field);
    rows.push(row);
  }

  while (rows.length > 0 && rows[rows.length - 1].length === 1 && rows[rows.length - 1][0] === '') {
    rows.pop();
  }

  if (rows.length < 2) return [];

  const headers = rows[0];
  const data = [];
  for (let r = 1; r < rows.length; r++) {
    if (rows[r].length === 1 && rows[r][0] === '') continue;
    const obj = {};
    headers.forEach((h, j) => { obj[h.trim()] = rows[r][j] != null ? rows[r][j] : ''; });
    data.push(obj);
  }
  return data;
}

function loadLocalDataset(key, data) {
  const s = state[key];
  s.localData = data;
  s.total = data.length;
  s.idx = 0;

  const prefix = getPrefix(key);
  const totalEl = document.getElementById(`${prefix}-total`);
  if (totalEl) totalEl.textContent = data.length.toLocaleString();

  // Bulk-track stats for local data so filters populate immediately
  if (key !== 'bench' && typeof statsTracker !== 'undefined' && statsTracker[key]) {
    const t = statsTracker[key];
    for (const item of data) {
      if (item.model) t.models[item.model] = (t.models[item.model] || 0) + 1;
      if (item.source) t.sources[item.source] = (t.sources[item.source] || 0) + 1;
    }
    updateFilterOptions(key);
    updateMiniStats(key);
  }

  renderItemFromData(key, data[0]);
  updateControls(key);
}

// ── Rendering ────────────────────────────────────────────────────
function escapeHtml(str) {
  if (str == null) return '';
  return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// Wraps bare LaTeX in $...$ so KaTeX will render it.
// If the string already has math delimiters ($, \[, \() it is returned as-is.
// Plain non-math strings are HTML-escaped normally.
function renderMathText(str) {
  str = String(str == null ? '' : str);
  if (!str) return '';
  // Check for real math delimiters (negative lookbehind avoids matching \\[ or \\()
  //   \\[2ex] is a LaTeX line-break → contains \[ but preceded by \, so ignored
  //   \( f(x) \)  → \( NOT preceded by \ → real inline delimiter
  const hasDelim = str.includes('$') ||
                   /(?<!\\)\\\(/.test(str) ||
                   /(?<!\\)\\\[/.test(str);
  if (hasDelim) return escapeHtml(str); // KaTeX will process delimiters in the text node

  // Bare LaTeX (\command, ^, _, {}, etc.) — wrap in appropriate delimiters
  if (/\\[a-zA-Z{]/.test(str) || /[\^_{}]/.test(str)) {
    // HTML-escape < > & to prevent innerHTML parsing corruption
    const safe = str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    // \begin{...} environments and multi-line content need display math
    if (/\\begin\{/.test(str) || str.includes('\n')) return '\\[' + safe + '\\]';
    return '$' + safe + '$';
  }
  // Plain text
  return escapeHtml(str);
}

// Prose text with embedded bare LaTeX tokens (e.g. analyst notes).
// Splits on whitespace; wraps math-looking tokens in $...$; HTML-escapes plain words.
// Also renders **bold** markdown.
function renderProseWithMath(text) {
  if (!text) return '';
  let result;
  if (/\$|\\\[|\\\(/.test(text)) {
    // Text already has math delimiters — HTML-escape (KaTeX will process $...$)
    result = escapeHtml(text);
  } else {
    // Whitespace-split; wrap bare LaTeX tokens in $...$; escape plain words
    result = text.split(/(\s+)/).map(part => {
      if (/^\s+$/.test(part)) return part;
      if (/[_^{}]/.test(part) || /\\[a-zA-Z]/.test(part)) return `$${part}$`;
      return escapeHtml(part);
    }).join('');
  }
  // **bold** → <strong>bold</strong> — always applied (asterisks survive escapeHtml)
  return result.replace(/\*\*([\s\S]+?)\*\*/g, '<strong>$1</strong>');
}

function renderMath(container) {
  if (typeof renderMathInElement === 'function') {
    renderMathInElement(container, {
      delimiters: [
        { left: '$$', right: '$$', display: true },
        { left: '$', right: '$', display: false },
        { left: '\\[', right: '\\]', display: true },
        { left: '\\(', right: '\\)', display: false },
      ],
      throwOnError: false,
    });
  }
}

function renderMessages(messages) {
  if (!messages || !Array.isArray(messages) || messages.length === 0) {
    return '<span style="color:#484f58">No messages</span>';
  }
  let html = '<div class="messages-container">';
  for (const msg of messages) {
    const role = msg.role || 'unknown';
    const roleClass = `msg-role-${role}`;
    const msgClass = `msg-${role}`;

    html += `<div class="msg ${msgClass}">`;
    html += `<div class="msg-role ${roleClass}">${escapeHtml(role)}</div>`;

    // Reasoning content (collapsible)
    if (msg.reasoning_content) {
      const id = 'r_' + Math.random().toString(36).slice(2, 10);
      html += `<span class="reasoning-toggle" onclick="toggleReasoning('${id}')">Show reasoning</span>`;
      html += `<div class="reasoning-content" id="${id}">${escapeHtml(msg.reasoning_content)}</div>`;
    }

    // Tool calls
    if (msg.tool_calls && msg.tool_calls.length > 0) {
      for (const tc of msg.tool_calls) {
        const fname = tc.function?.name || 'tool';
        let args = tc.function?.arguments || '';
        let isPython = false;
        // Parse JSON string args and pretty-print code fields
        if (typeof args === 'string') {
          try {
            const parsed = JSON.parse(args);
            if (parsed && typeof parsed.code === 'string') {
              args = parsed.code;
              isPython = true;
            } else {
              args = JSON.stringify(parsed, null, 2);
            }
          } catch (e) { /* keep raw string */ }
        } else if (typeof args === 'object') {
          if (typeof args.code === 'string') {
            args = args.code;
            isPython = true;
          } else {
            args = JSON.stringify(args, null, 2);
          }
        }
        html += `<div style="margin-top:0.3rem"><span style="color:#f5a623;font-size:0.75rem">${escapeHtml(fname)}()</span></div>`;
        if (isPython) {
          html += `<pre class="tool-code"><code class="language-python">${escapeHtml(args)}</code></pre>`;
        } else {
          html += `<div class="tool-code">${escapeHtml(args)}</div>`;
        }
      }
    }

    // Content - collapsible for tool messages with long output
    if (msg.content && !(typeof msg.content === 'string' && msg.content.trim() === '')) {
      const text = typeof msg.content === 'string' ? msg.content : JSON.stringify(msg.content, null, 2);
      const isLongTool = role === 'tool' && text.length > 600;
      if (isLongTool) {
        const tid = 'to_' + Math.random().toString(36).slice(2, 10);
        html += `<div class="tool-output-wrapper">`;
        html += `<div class="tool-output-collapsed" id="${tid}" style="margin-top:0.2rem">${escapeHtml(text)}</div>`;
        html += `<button class="tool-output-toggle" onclick="toggleToolOutput('${tid}')">Show full output</button>`;
        html += `</div>`;
      } else {
        html += `<div style="margin-top:0.2rem">${escapeHtml(text)}</div>`;
      }
    }

    html += '</div>';
  }
  html += '</div>';
  return html;
}

// ── Judge helpers ─────────────────────────────────────────────────
function parseJSONField(val) {
  if (val == null) return null;
  if (typeof val === 'object') return val;
  if (typeof val === 'string') {
    try { return JSON.parse(val); } catch (e) { return null; }
  }
  return null;
}

// Mirror of src/prompts/judge.py:parse_judge_response
function parseJudgeResponse(text) {
  if (!text) return null;
  // 1. Fenced code block
  const fenced = text.match(/```(?:json)?\s*([\s\S]*?)```/);
  if (fenced) {
    try {
      const r = JSON.parse(fenced[1]);
      if (r && typeof r === 'object' && 'scores' in r) return r;
    } catch (e) {}
  }
  // 2. Scan right-to-left for balanced {...} containing a 'scores' key
  const positions = [];
  for (let i = 0; i < text.length; i++) {
    if (text[i] === '{') positions.push(i);
  }
  for (let pi = positions.length - 1; pi >= 0; pi--) {
    const pos = positions[pi];
    let depth = 0, end = -1;
    for (let i = pos; i < text.length; i++) {
      if (text[i] === '{') depth++;
      else if (text[i] === '}') { depth--; if (depth === 0) { end = i + 1; break; } }
    }
    if (end === -1) continue;
    try {
      const r = JSON.parse(text.slice(pos, end));
      if (r && typeof r === 'object' && 'scores' in r) return r;
    } catch (e) {}
  }
  return null;
}

function groupMessagesIntoTurns(messages) {
  if (!messages || !Array.isArray(messages)) return { leading: [], turns: [] };
  const leading = [];
  let i = 0;
  // Collect leading user messages (the question)
  while (i < messages.length && messages[i].role === 'user') {
    leading.push(messages[i]);
    i++;
  }
  const turns = [];
  let turnIdx = 0;
  while (i < messages.length) {
    const msg = messages[i];
    if (msg.role === 'assistant') {
      const group = [msg];
      while (i + 1 < messages.length && (messages[i + 1].role === 'tool' || messages[i + 1].role === 'function')) {
        i++;
        group.push(messages[i]);
      }
      turns.push({ turnIdx, messages: group });
      turnIdx++;
    } else {
      // Stray non-assistant message — treat as part of leading
      leading.push(msg);
    }
    i++;
  }
  return { leading, turns };
}

function judgeScoreClass(score) {
  if (score >= 0.7) return 'judge-score-good';
  if (score >= 0.5) return 'judge-score-mid';
  return 'judge-score-bad';
}

function renderJudgePanelHtml(turnIdx, turnAgg, judgeR2sByJudge, judgeMessagesByJudge) {
  const score = turnAgg ? turnAgg.turn_score : null;
  const scoreStr = score != null ? Number(score).toFixed(2) : '—';
  const scoreClass = score != null ? judgeScoreClass(score) : '';

  let html = `<div class="judge-resize-handle"></div>`;
  html += `<div class="turn-judge-panel">`;
  html += `<div class="judge-panel-header">Turn ${turnIdx + 1} · <span class="${scoreClass}">${scoreStr}</span></div>`;

  // Criterion table
  const criteria = ['math_correct', 'code_integrity', 'contribution', 'output_interp'];
  const criteriaLabels = { math_correct: 'Math Correct', code_integrity: 'Code Integrity', contribution: 'Contribution', output_interp: 'Output Interp' };
  html += '<table class="judge-criteria-table">';
  for (const c of criteria) {
    const val = turnAgg ? turnAgg[c] : null;
    if (val == null) continue;
    const cls = judgeScoreClass(val);
    html += `<tr><td class="crit-name">${criteriaLabels[c]}</td><td class="crit-score ${cls}">${Number(val).toFixed(2)}</td></tr>`;
  }
  html += '</table>';

  // Judge notes (collapsible)
  const notesId = 'jn_' + Math.random().toString(36).slice(2, 10);
  html += `<span class="judge-section-toggle" onclick="toggleJudgeSection('${notesId}')">▶ Judge notes</span>`;
  html += `<div class="judge-section-body" id="${notesId}">`;
  for (let j = 0; j < 4; j++) {
    const r2list = judgeR2sByJudge[j];
    const entry = r2list ? r2list.find(e => e.turn_idx === turnIdx) : null;
    const notes = entry ? entry.notes : null;
    if (notes && typeof notes === 'object' && Object.keys(notes).length > 0) {
      html += `<div class="judge-note-card">`;
      html += `<div class="judge-note-header">Judge ${j}</div>`;
      for (const [crit, note] of Object.entries(notes)) {
        html += `<div class="judge-note-row">`;
        html += `<span class="judge-note-crit">${escapeHtml(crit)}</span>`;
        html += `<span class="judge-note-text">${escapeHtml(String(note))}</span>`;
        html += `</div>`;
      }
      html += `</div>`;
    }
  }
  html += '</div>';

  // View conversations toggle — rendered OUTSIDE the panel, full-width below the turn
  const convId = 'jc_' + Math.random().toString(36).slice(2, 10);
  html += `<span class="judge-section-toggle" onclick="toggleJudgeSection('${convId}')">▶ View conversations</span>`;

  // Close the judge panel div here
  html += '</div>';

  // Full-width conversation section with judge tab selector
  html += `<div class="judge-conv-full" id="${convId}">`;

  // Tab bar for selecting which judge to view
  html += `<div class="judge-conv-tabs" id="${convId}_tabs">`;
  for (let j = 0; j < 4; j++) {
    const msgMap = judgeMessagesByJudge[j];
    const turnKey = `turn_${turnIdx}`;
    const msgs = msgMap ? msgMap[turnKey] : null;
    const hasData = msgs && Array.isArray(msgs) && msgs.length > 0;
    html += `<button class="judge-conv-tab${j === 0 ? ' active' : ''}${!hasData ? ' disabled' : ''}" `;
    html += `onclick="switchJudgeConvTab('${convId}', ${j})" `;
    html += `${!hasData ? 'disabled' : ''}>Judge ${j}</button>`;
  }
  html += `</div>`;

  // One card per judge (only first is visible by default)
  for (let j = 0; j < 4; j++) {
    const msgMap = judgeMessagesByJudge[j];
    const turnKey = `turn_${turnIdx}`;
    const msgs = msgMap ? msgMap[turnKey] : null;
    if (msgs && Array.isArray(msgs) && msgs.length > 0) {
      html += `<div class="judge-conv-card" id="${convId}_j${j}" style="${j === 0 ? '' : 'display:none'}">`;
      html += `<div class="judge-conv-card-header">Judge ${j} — Turn ${turnIdx + 1} Deliberation</div>`;
      for (const m of msgs) {
        const role = m.role || 'unknown';
        const text = typeof m.content === 'string' ? m.content : JSON.stringify(m.content || '');
        const reasoning = m.reasoning_content || '';
        html += `<div class="judge-conv-msg role-${role}">`;
        html += `<div class="judge-conv-role">${escapeHtml(role)}</div>`;

        // Reasoning content (collapsible) — same pattern as main trace
        if (reasoning) {
          const rId = 'jr_' + Math.random().toString(36).slice(2, 10);
          html += `<span class="reasoning-toggle" onclick="toggleReasoning('${rId}')">Show reasoning</span>`;
          html += `<div class="reasoning-content" id="${rId}">${escapeHtml(reasoning)}</div>`;
        }

        // Main content with expand toggle for very long messages
        const LIMIT = 3000;
        const isLong = text.length > LIMIT;
        const shortText = isLong ? text.slice(0, LIMIT) : text;
        const longId = 'jcm_' + Math.random().toString(36).slice(2, 10);
        if (isLong) {
          html += `<div class="judge-conv-text">${escapeHtml(shortText)}<span class="judge-conv-more" id="${longId}" style="display:none">${escapeHtml(text.slice(LIMIT))}</span></div>`;
          html += `<span class="judge-conv-expand" onclick="var el=document.getElementById('${longId}');if(el.style.display==='none'){el.style.display='inline';this.textContent='Show less'}else{el.style.display='none';this.textContent='Show full (${Math.round(text.length/1000)}k chars)'}">Show full (${Math.round(text.length/1000)}k chars)</span>`;
        } else {
          html += `<div class="judge-conv-text">${escapeHtml(text)}</div>`;
        }
        html += `</div>`;
      }
      // Close button at bottom of card
      html += `<div class="judge-conv-close-row">`;
      html += `<button class="judge-conv-close-btn" onclick="toggleJudgeSection('${convId}')">✕ Close conversation</button>`;
      html += `</div>`;
      html += `</div>`;
    }
  }
  html += '</div>';

  // Note: the panel </div> was already closed above before the conversation section
  return html;
}

function renderMessagesWithJudge(item) {
  const messages = item.messages;
  const turnAggregated = parseJSONField(item.turn_aggregated) || [];

  // Read from consolidated judges_messages column or legacy per-judge columns.
  // judgeR2s is derived on-the-fly from the R2 assistant message in each conversation.
  const allMsgs = parseJSONField(item.judges_messages);
  const judgeR2s = [];
  const judgeMsgs = [];
  for (let j = 0; j < 4; j++) {
    const msgs = (allMsgs && allMsgs[j]) ? allMsgs[j] : (parseJSONField(item[`judge_${j}_messages`]) || {});
    judgeMsgs.push(msgs);
    // Legacy fallback: if judges_r2 column still present (old files), use it directly
    const legacyR2 = parseJSONField(item.judges_r2);
    if (legacyR2 && legacyR2[j]) {
      judgeR2s.push(legacyR2[j]);
      continue;
    }
    const legacyPerJudge = parseJSONField(item[`judge_${j}_r2`]);
    if (legacyPerJudge) {
      judgeR2s.push(legacyPerJudge);
      continue;
    }
    // Derive R2 results by parsing the R2 assistant message (index 3) per turn
    const r2list = [];
    for (const [turnKey, turnMsgs] of Object.entries(msgs)) {
      const turn_idx = parseInt(turnKey.split('_')[1], 10);
      const r2msg = turnMsgs[3];
      const fullText = ((r2msg && r2msg.reasoning_content) || '') + ((r2msg && r2msg.content) || '');
      const parsed = parseJudgeResponse(fullText) || {};
      r2list.push({ turn_idx, scores: parsed.scores || {}, notes: parsed.notes || {} });
    }
    judgeR2s.push(r2list);
  }

  const { leading, turns } = groupMessagesIntoTurns(messages);

  // Build lookup for turn_aggregated by turn_idx
  const aggMap = {};
  for (const agg of turnAggregated) {
    aggMap[agg.turn_idx] = agg;
  }

  let html = '<div class="messages-container">';

  // Render leading user messages (no judge panel)
  for (const msg of leading) {
    html += renderSingleMessage(msg);
  }

  // Render each turn with judge panel
  for (const turn of turns) {
    html += '<div class="turn-group">';
    // Messages column
    html += '<div class="turn-messages-col">';
    for (const msg of turn.messages) {
      html += renderSingleMessage(msg);
    }
    html += '</div>';
    // Judge panel column
    html += renderJudgePanelHtml(turn.turnIdx, aggMap[turn.turnIdx] || null, judgeR2s, judgeMsgs);
    html += '</div>';
  }

  html += '</div>';
  return html;
}

function renderSingleMessage(msg) {
  const role = msg.role || 'unknown';
  const roleClass = `msg-role-${role}`;
  const msgClass = `msg-${role}`;
  let html = `<div class="msg ${msgClass}">`;
  html += `<div class="msg-role ${roleClass}">${escapeHtml(role)}</div>`;

  if (msg.reasoning_content) {
    const id = 'r_' + Math.random().toString(36).slice(2, 10);
    html += `<span class="reasoning-toggle" onclick="toggleReasoning('${id}')">Show reasoning</span>`;
    html += `<div class="reasoning-content" id="${id}">${escapeHtml(msg.reasoning_content)}</div>`;
  }

  if (msg.tool_calls && msg.tool_calls.length > 0) {
    for (const tc of msg.tool_calls) {
      const fname = tc.function?.name || 'tool';
      let args = tc.function?.arguments || '';
      let isPython = false;
      if (typeof args === 'string') {
        try {
          const parsed = JSON.parse(args);
          if (parsed && typeof parsed.code === 'string') { args = parsed.code; isPython = true; }
          else { args = JSON.stringify(parsed, null, 2); }
        } catch (e) { /* keep raw string */ }
      } else if (typeof args === 'object') {
        if (typeof args.code === 'string') { args = args.code; isPython = true; }
        else { args = JSON.stringify(args, null, 2); }
      }
      html += `<div style="margin-top:0.3rem"><span style="color:#f5a623;font-size:0.75rem">${escapeHtml(fname)}()</span></div>`;
      if (isPython) {
        html += `<pre class="tool-code"><code class="language-python">${escapeHtml(args)}</code></pre>`;
      } else {
        html += `<div class="tool-code">${escapeHtml(args)}</div>`;
      }
    }
  }

  if (msg.content && !(typeof msg.content === 'string' && msg.content.trim() === '')) {
    const text = typeof msg.content === 'string' ? msg.content : JSON.stringify(msg.content, null, 2);
    const isLongTool = role === 'tool' && text.length > 600;
    if (isLongTool) {
      const tid = 'to_' + Math.random().toString(36).slice(2, 10);
      html += `<div class="tool-output-wrapper">`;
      html += `<div class="tool-output-collapsed" id="${tid}" style="margin-top:0.2rem">${escapeHtml(text)}</div>`;
      html += `<button class="tool-output-toggle" onclick="toggleToolOutput('${tid}')">Show full output</button>`;
      html += `</div>`;
    } else {
      html += `<div style="margin-top:0.2rem">${escapeHtml(text)}</div>`;
    }
  }

  html += '</div>';
  return html;
}

// Switch judge conversation tab — show only selected judge's card
window.switchJudgeConvTab = function(convId, judgeIdx) {
  // Hide all cards, show selected
  for (let j = 0; j < 4; j++) {
    const card = document.getElementById(`${convId}_j${j}`);
    if (card) card.style.display = (j === judgeIdx) ? '' : 'none';
  }
  // Update tab active state
  const tabs = document.getElementById(`${convId}_tabs`);
  if (tabs) {
    tabs.querySelectorAll('.judge-conv-tab').forEach((btn, i) => {
      btn.classList.toggle('active', i === judgeIdx);
    });
  }
};

window.toggleJudgeSection = function(id) {
  const el = document.getElementById(id);
  if (el) {
    const isOpen = el.classList.contains('open');
    el.classList.toggle('open');
    // Find the toggle button: it's either the previousElementSibling directly,
    // or the last child of the previousElementSibling (for conv-full sections)
    let toggle = el.previousElementSibling;
    if (toggle && !toggle.classList.contains('judge-section-toggle')) {
      // The conv-full section is after the panel div; the toggle is the last child of that panel
      const lastChild = toggle.lastElementChild;
      if (lastChild && lastChild.classList.contains('judge-section-toggle')) {
        toggle = lastChild;
      }
    }
    if (toggle && toggle.classList.contains('judge-section-toggle')) {
      toggle.textContent = (isOpen ? '▶ ' : '▼ ') + toggle.textContent.slice(2);
    }
  }
};

function renderDatasetItem(item, key) {
  let html = '';
  const stKey = (key === 'preview') ? 'preview' : 'stage2';
  const isJudgeMode = state[stKey].showJudge && item.trace_score != null;

  // Meta pills
  const pills = [];
  if (item.model) pills.push(['Model', item.model]);
  if (item.source) pills.push(['Source', item.source]);
  if (item.topic) pills.push(['Topic', item.topic]);
  if (item.success_rate != null) pills.push(['Success Rate', Number(item.success_rate).toFixed(3)]);
  if (item.attempts != null) pills.push(['Attempts', item.attempts]);
  if (item.tokens_len != null) pills.push(['Tokens', Number(item.tokens_len).toLocaleString()]);
  if (item.transform != null) pills.push(['Transformed', item.transform ? 'Yes' : 'No']);
  if (item.license) pills.push(['License', item.license]);

  if (pills.length > 0 || isJudgeMode) {
    html += '<div class="meta-row">';
    for (const [k, v] of pills) {
      html += `<span class="meta-pill"><span class="pill-key">${escapeHtml(k)}</span> <span class="pill-val">${escapeHtml(String(v))}</span></span>`;
    }
    // Judge score pill
    if (isJudgeMode) {
      const ts = Number(item.trace_score);
      const cls = judgeScoreClass(ts);
      html += `<span class="meta-pill"><span class="pill-key">Judge Score</span> <span class="pill-val ${cls}">${ts.toFixed(3)}</span></span>`;
    }
    html += '</div>';
  }

  // Question
  if (item.question) {
    html += '<div class="field-group">';
    html += '<div class="field-label">Question</div>';
    html += `<div class="field-value question-text">${escapeHtml(item.question)}</div>`;
    html += '</div>';
  }

  // Answer
  if (item.answer != null) {
    html += '<div class="field-group">';
    html += '<div class="field-label">Answer</div>';
    html += `<div class="field-value answer-text">${escapeHtml(String(item.answer))}</div>`;
    html += '</div>';
  }

  // Messages — with or without judge panels
  if (item.messages) {
    html += '<div class="field-group">';
    html += '<div class="field-label">Messages / TIR Trace</div>';
    if (isJudgeMode) {
      html += `<div class="field-value messages-field">${renderMessagesWithJudge(item)}</div>`;
    } else {
      html += `<div class="field-value messages-field">${renderMessages(item.messages)}</div>`;
    }
    html += '</div>';
  }

  // Libraries
  if (item.libraries && Array.isArray(item.libraries) && item.libraries.length > 0) {
    html += '<div class="field-group">';
    html += '<div class="field-label">Libraries</div>';
    html += '<div class="meta-row">';
    for (const lib of item.libraries) {
      html += `<span class="meta-pill"><span class="pill-val">${escapeHtml(lib)}</span></span>`;
    }
    html += '</div></div>';
  }

  // Tools run time
  if (item.tools_run_time && Array.isArray(item.tools_run_time) && item.tools_run_time.length > 0) {
    const total = item.tools_run_time.reduce((a, b) => a + b, 0);
    html += '<div class="field-group">';
    html += `<div class="field-label">Tool Execution Times (total: ${total.toFixed(2)}s)</div>`;
    html += `<div class="field-value">${item.tools_run_time.map(t => t.toFixed(3) + 's').join(', ')}</div>`;
    html += '</div>';
  }

  // Tool schema
  if (item.tool) {
    html += '<div class="field-group">';
    html += '<div class="field-label">Tool Schema</div>';
    const toolStr = typeof item.tool === 'string' ? item.tool : JSON.stringify(item.tool, null, 2);
    html += `<div class="field-value" style="font-family:monospace;font-size:0.8rem">${escapeHtml(toolStr)}</div>`;
    html += '</div>';
  }

  // Metadata
  if (item.metadata) {
    html += '<div class="field-group">';
    html += '<div class="field-label">Metadata</div>';
    const metaStr = typeof item.metadata === 'string' ? item.metadata : JSON.stringify(item.metadata, null, 2);
    html += `<div class="field-value" style="font-family:monospace;font-size:0.8rem">${escapeHtml(metaStr)}</div>`;
    html += '</div>';
  }

  return html;
}

function renderBenchItem(item) {
  let html = '';

  // ID pill
  if (item.id != null) {
    html += '<div class="meta-row">';
    html += `<span class="meta-pill"><span class="pill-key">ID</span> <span class="pill-val">${escapeHtml(String(item.id))}</span></span>`;
    html += '</div>';
  }

  // Problem + Original Problem side by side
  html += '<div class="bench-field-row">';

  html += '<div class="field-group">';
  html += '<div class="field-label">Problem</div>';
  html += `<div class="field-value question-text">${escapeHtml(item.problem || '')}</div>`;
  html += '</div>';

  html += '<div class="field-group">';
  html += '<div class="field-label">Original Problem</div>';
  html += `<div class="field-value question-text">${escapeHtml(item.original_problem || '')}</div>`;
  html += '</div>';

  html += '</div>';

  // Answer + Original Answer side by side
  html += '<div class="bench-field-row">';

  html += '<div class="field-group">';
  html += '<div class="field-label">Answer</div>';
  html += `<div class="field-value answer-text">${escapeHtml(String(item.answer || ''))}</div>`;
  html += '</div>';

  html += '<div class="field-group">';
  html += '<div class="field-label">Original Answer</div>';
  html += `<div class="field-value answer-text">${escapeHtml(String(item.original_answer || ''))}</div>`;
  html += '</div>';

  html += '</div>';

  return html;
}

function renderErrorTraceItem(item) {
  let html = '';

  // Meta pills: model, source
  const pills = [];
  if (item.model) pills.push(['Model', item.model]);
  if (item.source) pills.push(['Source', item.source]);

  if (pills.length > 0) {
    html += '<div class="meta-row">';
    for (const [k, v] of pills) {
      html += `<span class="meta-pill"><span class="pill-key">${escapeHtml(k)}</span> <span class="pill-val">${escapeHtml(String(v))}</span></span>`;
    }
    html += '</div>';
  }

  // Question
  if (item.question) {
    html += '<div class="field-group">';
    html += '<div class="field-label">Question</div>';
    html += `<div class="field-value question-text">${escapeHtml(item.question)}</div>`;
    html += '</div>';
  }

  // Answer (ground truth)
  if (item.answer != null) {
    html += '<div class="field-group">';
    html += '<div class="field-label">Answer (Ground Truth)</div>';
    html += `<div class="field-value answer-text">${escapeHtml(String(item.answer))}</div>`;
    html += '</div>';
  }

  // Extracted answer (what the model produced)
  if (item.extracted_answer != null) {
    html += '<div class="field-group">';
    html += '<div class="field-label">Extracted Answer (Model Output)</div>';
    html += `<div class="field-value" style="font-size:1.1rem;font-weight:700;color:var(--orange)">${escapeHtml(String(item.extracted_answer))}</div>`;
    html += '</div>';
  }

  // Messages (full error trace)
  if (item.messages) {
    html += '<div class="field-group">';
    html += '<div class="field-label">Messages / Error Trace</div>';
    html += `<div class="field-value messages-field">${renderMessages(item.messages)}</div>`;
    html += '</div>';
  }

  return html;
}

// ── Verifier solution grid ───────────────────────────────────────
function extractVerifierAnswer(msgs) {
  for (let i = msgs.length - 1; i >= 0; i--) {
    if (msgs[i].role !== 'assistant') continue;
    const raw = msgs[i].content;

    // Case 1: content is already a parsed object (HF may return structured columns)
    if (raw && typeof raw === 'object' && !Array.isArray(raw)) {
      if (raw.new_answer !== undefined) return String(raw.new_answer).trim();
      if (raw.answer    !== undefined) return String(raw.answer).trim();
    }

    // Case 2: content is a JSON string — transformation pipeline format
    if (typeof raw === 'string') {
      try {
        const parsed = JSON.parse(raw);
        if (parsed && typeof parsed === 'object') {
          if (parsed.new_answer !== undefined) return String(parsed.new_answer).trim();
          if (parsed.answer    !== undefined) return String(parsed.answer).trim();
        }
      } catch (e) { /* not JSON */ }

      // Case 3: plain text with \boxed{} (fallback)
      const matches = [...raw.matchAll(/\\boxed\{([^}]{0,80})\}/g)];
      if (matches.length > 0) return matches[matches.length - 1][1].trim();
    }
  }
  return null;
}


// Shared helper: build answer→color map from verifier solution data
function getAnswerColorMap(solutionData) {
  const solutions = parseJSONField(solutionData);
  if (!solutions || !Array.isArray(solutions)) return {};
  const answers = solutions.map(sol => extractVerifierAnswer(Array.isArray(sol) ? sol : []));
  const uniqueAnswers = [...new Set(answers.filter(a => a !== null))];
  const map = {};
  uniqueAnswers.forEach((ans, i) => { map[ans] = ANSWER_COLORS[i % ANSWER_COLORS.length]; });
  return map;
}

function renderSolutionGrid(solutionData, colorMap) {

  const solutions = parseJSONField(solutionData);
  if (!solutions || !Array.isArray(solutions) || solutions.length === 0) return '';

  const answers = solutions.map(sol => extractVerifierAnswer(Array.isArray(sol) ? sol : []));
  // Use pre-computed colorMap if provided, otherwise compute here
  const answerColor = colorMap || getAnswerColorMap(solutionData);

  // Build legend counts
  const legendCounts = {};
  for (const ans of answers) {
    const k = ans ?? '(no answer)';
    legendCounts[k] = (legendCounts[k] || 0) + 1;
  }

  let html = '<div class="field-group">';
  html += `<div class="field-label">Verifier Solutions (${solutions.length} runs)</div>`;

  // Legend
  html += '<div class="solution-legend">';
  for (const [ans, cnt] of Object.entries(legendCounts).sort((a, b) => b[1] - a[1])) {
    const color = answerColor[ans] || '#9ca3af';
    html += `<span class="solution-legend-item" style="border-left:3px solid ${color}">`;
    html += `<span class="solution-legend-ans" style="color:${color}">${escapeHtml(ans)}</span>`;
    html += `<span class="solution-legend-cnt">×${cnt}</span>`;
    html += '</span>';
  }
  html += '</div>';

  // Grid of cards
  html += '<div class="solution-grid">';
  solutions.forEach((sol, i) => {
    const ans = answers[i];
    const color = (ans && answerColor[ans]) ? answerColor[ans] : '#9ca3af';
    const bodyId = 'sb_' + Math.random().toString(36).slice(2, 10);
    const togId  = 'st_' + Math.random().toString(36).slice(2, 10);

    html += `<div class="solution-card" style="border-left-color:${color}">`;
    html += '<div class="solution-card-header">';
    html += `<span class="solution-run-badge" style="background:${color}">Run ${i + 1}</span>`;
    html += `<span class="solution-ans-text" style="color:${color}">${ans !== null ? escapeHtml(ans) : '—'}</span>`;
    html += `<span class="solution-card-toggle" id="${togId}" onclick="toggleSolutionBody('${bodyId}','${togId}')">&#9654; Show</span>`;
    html += '</div>';

    html += `<div class="solution-card-body" id="${bodyId}" style="display:none">`;
    const msgs = Array.isArray(sol) ? sol : [];
    for (const msg of msgs) {
      const role = msg.role || 'unknown';
      const content = typeof msg.content === 'string' ? msg.content : JSON.stringify(msg.content || '');
      const LIMIT = 1500;
      html += `<div class="solution-msg role-${role}">`;
      html += `<div class="solution-msg-role">${escapeHtml(role)}</div>`;

      // Reasoning ABOVE content (same pattern as Stage 1/2)
      const rc = msg.reasoning_content;
      if (rc) {
        const rcText = typeof rc === 'string' ? rc : JSON.stringify(rc);
        const rcId = 'rc_' + Math.random().toString(36).slice(2, 10);
        html += `<span class="reasoning-toggle" onclick="toggleReasoning('${rcId}')">Show reasoning (\u2248${Math.round(rcText.length / 1000)}k chars)</span>`;
        html += `<div class="reasoning-content" id="${rcId}">${escapeHtml(rcText)}</div>`;
      }

      // Main content
      const moreId = 'sm_' + Math.random().toString(36).slice(2, 10);
      if (content.length > LIMIT) {
        html += `<div class="solution-msg-text">${escapeHtml(content.slice(0, LIMIT))}<span id="${moreId}" style="display:none">${escapeHtml(content.slice(LIMIT))}</span></div>`;
        html += `<span class="solution-expand" onclick="var e=document.getElementById('${moreId}'),s=e.style.display==='none';e.style.display=s?'inline':'none';this.textContent=s?'Show less':'Show more (${Math.round(content.length/1000)}k chars)'">Show more (${Math.round(content.length/1000)}k chars)</span>`;
      } else {
        html += `<div class="solution-msg-text">${escapeHtml(content)}</div>`;
      }
      html += '</div>';
    }

    html += '</div></div>'; // body + card
  });
  html += '</div></div>'; // grid + field-group
  return html;
}


function renderTransformFailureItem(item) {
  const CATEGORY_LABELS = {
    near_consensus:      'Near Consensus',
    precision_rounding:  'Precision / Rounding',
    symbolic_vs_numeric: 'Symbolic vs Numeric',
    sign_error:          'Sign Error',
    genuine_disagreement:'Genuine Disagreement',
    no_answers:          'No Answers',
  };
  const CATEGORY_COLORS = {
    near_consensus:      'var(--orange, #f39c12)',
    precision_rounding:  'var(--blue, #3498db)',
    symbolic_vs_numeric: '#9b59b6',
    sign_error:          'var(--red, #e74c3c)',
    genuine_disagreement:'#555',
    no_answers:          '#aaa',
  };

  const cat      = item.failure_category || 'genuine_disagreement';
  const catLabel = CATEGORY_LABELS[cat] || cat;
  const catColor = CATEGORY_COLORS[cat] || '#555';

  // Pre-compute answer→color map (shared with answer distribution + verifier grid)
  const answerColorMap = getAnswerColorMap(item.solution);

  // ── Meta pills ────────────────────────────────────────────────
  let html = '<div class="meta-row">';
  html += `<span class="meta-pill"><span class="pill-key">Category</span> <span class="pill-val" style="color:${catColor};font-weight:700">${escapeHtml(catLabel)}</span></span>`;
  html += `<span class="meta-pill"><span class="pill-key">Split</span> <span class="pill-val">${escapeHtml(item.split_info || '?')}</span></span>`;
  html += `<span class="meta-pill"><span class="pill-key">Runs</span> <span class="pill-val">${item.n_successful}/${item.n_total}</span></span>`;
  html += `<span class="meta-pill"><span class="pill-key">Unique answers</span> <span class="pill-val">${item.n_unique}</span></span>`;
  if (item.question_uuid) html += `<span class="meta-pill"><span class="pill-key">UUID</span> <span class="pill-val">${escapeHtml(item.question_uuid)}</span></span>`;
  html += '</div>';

  // ── 1. Problem ────────────────────────────────────────────────
  html += '<div class="field-group"><div class="field-label">Original Problem</div>';
  html += `<div class="field-value question-text">${renderMathText(item.original_problem)}</div></div>`;

  // ── 2. Answer ─────────────────────────────────────────────────
  html += '<div class="field-group"><div class="field-label">Original Answer</div>';
  html += `<div class="field-value answer-text" style="font-size:1.05rem">${renderMathText(item.original_answer)}</div></div>`;

  // ── 3. Answer distribution (verifier colors) ──────────────────
  const _answerCounts = parseJSONField(item.answer_counts);
  if (_answerCounts && Object.keys(_answerCounts).length > 0) {
    const entries = Object.entries(_answerCounts).sort((a, b) => b[1] - a[1]);
    const total = entries.reduce((s, [, c]) => s + c, 0);
    html += '<div class="field-group"><div class="field-label">Answer Distribution</div><div class="field-value">';
    html += '<div style="display:flex;flex-wrap:wrap;gap:0.5rem">';
    for (const [ans, cnt] of entries) {
      const pct   = Math.round(100 * cnt / total);
      const color = answerColorMap[String(ans)] || catColor;
      html += `<span style="background:${color};color:#fff;padding:0.3rem 0.7rem;border-radius:4px;font-weight:700">`;
      html += `${renderMathText(String(ans))} \u00d7${cnt} (${pct}%)`;
      html += '</span>';
    }
    html += '</div></div></div>';
  }

  // ── 4. Analyst review ─────────────────────────────────────────
  if (item.analyst_note) {
    html += '<div class="field-group"><div class="field-label" style="color:#27ae60">Analyst Review</div>';
    html += `<div class="field-value" style="background:#f0faf4;border-left:3px solid #27ae60;padding:0.75rem 1rem;border-radius:4px;white-space:pre-wrap">${renderProseWithMath(item.analyst_note)}</div>`;
    html += '</div>';
  }

  // ── 5. Transformation note ────────────────────────────────────
  if (item.transformation_note) {
    html += '<div class="field-group"><div class="field-label">Transformation Note</div>';
    html += `<div class="field-value" style="font-size:0.88rem;white-space:pre-wrap;line-height:1.55">${escapeHtml(item.transformation_note)}</div>`;
    html += '</div>';
  }

  // ── 6. Verifier solutions grid ────────────────────────────────
  html += renderSolutionGrid(item.solution, answerColorMap);

  return html;
}


window.toggleSolutionBody = function(bodyId, togId) {
  const body = document.getElementById(bodyId);
  const tog  = document.getElementById(togId);
  if (!body) return;
  const open = body.style.display !== 'none';
  body.style.display = open ? 'none' : 'block';
  // Expand card to full grid width when open so trace text is readable
  const card = body.closest ? body.closest('.solution-card') : null;
  if (card) card.style.gridColumn = open ? '' : '1 / -1';
  if (tog) tog.innerHTML = open ? '&#9654; Show' : '&#9660; Hide';
};


function renderItemFromData(key, item) {
  const prefix = getPrefix(key);
  const card = document.getElementById(`${prefix}-card`);
  if (!item) { card.innerHTML = '<div class="card-placeholder"><p>No data</p></div>'; return; }

  let html;
  if (key === 'bench') {
    html = renderBenchItem(item);
  } else if (key === 'error') {
    html = renderErrorTraceItem(item);
  } else if (key === 'tfail') {
    html = renderTransformFailureItem(item);
  } else {
    html = renderDatasetItem(item, key);
  }
  card.innerHTML = html;
  requestAnimationFrame(() => {
    renderMath(card);
    // Syntax highlighting for Python code blocks
    if (typeof Prism !== 'undefined') {
      Prism.highlightAllUnder(card);
    }
  });

  // Track stats for filters + mini-bar
  if (key !== 'bench') trackItemStats(key, item);
}

function updateControls(key) {
  const s = state[key];
  const prefix = getPrefix(key);

  document.getElementById(`${prefix}-counter`).textContent =
    s.total > 0 ? `${s.idx + 1} / ${s.total.toLocaleString()}` : '0 / 0';
  document.getElementById(`${prefix}-prev`).disabled = s.total <= 0;
  document.getElementById(`${prefix}-next`).disabled = s.total <= 0;
}

// ── Reasoning toggle ─────────────────────────────────────────────
window.toggleReasoning = function(id) {
  const el = document.getElementById(id);
  if (el) {
    el.classList.toggle('open');
    const toggle = el.previousElementSibling;
    if (toggle) toggle.textContent = el.classList.contains('open') ? 'Hide reasoning' : 'Show reasoning';
  }
};

// ── Tool output toggle (collapsible) ─────────────────────────────
window.toggleToolOutput = function(id) {
  const el = document.getElementById(id);
  if (el) {
    const isCollapsed = el.classList.contains('tool-output-collapsed');
    el.classList.toggle('tool-output-collapsed');
    const btn = el.parentElement.querySelector('.tool-output-toggle');
    if (btn) btn.textContent = isCollapsed ? 'Collapse output' : 'Show full output';
  }
};

// ── Navigation helpers ───────────────────────────────────────────
function goToIndex(key, idx) {
  const s = state[key];
  if (s.total <= 0) return;
  idx = ((idx % s.total) + s.total) % s.total;

  // Update shareable URL (without triggering hashchange)
  const newHash = `#${key}/${idx + 1}`;
  if (location.hash !== newHash) {
    history.replaceState(null, '', newHash);
  }

  if (key === 'tfail') {
    if (s.localData) {
      // Local file mode (filtered array)
      const arr = s._filtered || s.localData || [];
      s.idx = idx;
      renderItemFromData(key, arr[idx]);
      updateControls(key);
    } else if (s.hfSplit) {
      // HF streaming mode
      fetchAndRender(key, idx);
    }
    return;
  }

  if (s.localData) {
    s.idx = idx;
    renderItemFromData(key, s.localData[idx]);
    updateControls(key);
  } else if (s.hfSplit) {
    fetchAndRender(key, idx);
  } else if (key === 'bench') {
    // bench with local CSV
    s.idx = idx;
    if (s.cache.has(idx)) {
      renderItemFromData(key, s.cache.get(idx));
    }
    updateControls(key);
  }
}

// ── Wire up controls ─────────────────────────────────────────────
function setupControls(key) {
  const prefix = getPrefix(key);

  document.getElementById(`${prefix}-prev`).addEventListener('click', () => {
    if (key !== 'bench') goToFilteredIndex(key, -1);
    else goToIndex(key, state[key].idx - 1);
  });
  document.getElementById(`${prefix}-next`).addEventListener('click', () => {
    if (key !== 'bench') goToFilteredIndex(key, 1);
    else goToIndex(key, state[key].idx + 1);
  });
  document.getElementById(`${prefix}-go`).addEventListener('click', () => {
    const input = document.getElementById(`${prefix}-jump`);
    const val = parseInt(input.value, 10);
    if (val >= 1 && val <= state[key].total) {
      goToIndex(key, val - 1);
    }
    input.value = '';
  });
  document.getElementById(`${prefix}-random`).addEventListener('click', () => {
    if (state[key].total > 0) {
      goToIndex(key, Math.floor(Math.random() * state[key].total));
    }
  });
  document.getElementById(`${prefix}-jump`).addEventListener('keydown', (e) => {
    if (e.key === 'Enter') document.getElementById(`${prefix}-go`).click();
  });
}
setupControls('stage1');
setupControls('stage2');
setupControls('preview');
setupControls('bench');
setupControls('error');
setupControls('tfail');

// ── Judge toggle (shared for stage2 + preview) ───────────────────
function wireJudgeToggle(key) {
  const prefix = getPrefix(key);
  const checkbox = document.getElementById(`${prefix}-judge-toggle`);
  checkbox.addEventListener('change', () => {
    state[key].showJudge = checkbox.checked;
    if (state[key].total > 0) {
      const item = state[key].localData
        ? state[key].localData[state[key].idx]
        : state[key].cache.get(state[key].idx);
      if (item) renderItemFromData(key, item);
    }
  });
}
wireJudgeToggle('stage2');
wireJudgeToggle('preview');

// ── File inputs (fallback) ───────────────────────────────────────
document.getElementById('s1-file').addEventListener('change', (e) => {
  readFile(e.target.files[0], 'stage1', 'jsonl');
});
document.getElementById('s2-file').addEventListener('change', (e) => {
  readFile(e.target.files[0], 'stage2', 'jsonl');
});
document.getElementById('pv-file').addEventListener('change', (e) => {
  readFile(e.target.files[0], 'preview', 'jsonl');
});
document.getElementById('b-file').addEventListener('change', (e) => {
  readFile(e.target.files[0], 'bench', 'csv');
});
document.getElementById('et-file').addEventListener('change', (e) => {
  readFile(e.target.files[0], 'error', 'jsonl');
});

function readFile(file, key, format) {
  if (!file) return;
  const reader = new FileReader();
  reader.onload = (e) => {
    const text = e.target.result;
    let data;
    if (format === 'csv') {
      data = parseCSV(text);
    } else {
      data = parseJSONL(text);
    }
    loadLocalDataset(key, data);
  };
  reader.readAsText(file);
}

// ── Drag & Drop ──────────────────────────────────────────────────
let dragCounter = 0;
document.addEventListener('dragenter', (e) => {
  e.preventDefault(); dragCounter++;
  document.body.classList.add('drag-over');
});
document.addEventListener('dragleave', (e) => {
  e.preventDefault(); dragCounter--;
  if (dragCounter <= 0) { document.body.classList.remove('drag-over'); dragCounter = 0; }
});
document.addEventListener('dragover', (e) => e.preventDefault());
document.addEventListener('drop', (e) => {
  e.preventDefault(); dragCounter = 0;
  document.body.classList.remove('drag-over');
  const file = e.dataTransfer.files[0];
  if (!file) return;

  const name = file.name.toLowerCase();
  if (name.includes('bench') || name.endsWith('.csv')) {
    readFile(file, 'bench', 'csv');
    location.hash = 'bench';
  } else if (name.includes('error') || name.includes('errortrace')) {
    readFile(file, 'error', 'jsonl');
    location.hash = 'error';
  } else if (name.includes('preview')) {
    readFile(file, 'preview', 'jsonl');
    location.hash = 'preview';
  } else if (name.includes('stage2') || name.includes('s2')) {
    readFile(file, 'stage2', 'jsonl');
    location.hash = 'stage2';
  } else {
    readFile(file, 'stage1', 'jsonl');
    location.hash = 'stage1';
  }
});

// ── Auto-load AstralBench CSV ────────────────────────────────────
function loadBenchCSV() {
  if (state.bench.localData || state.bench.loading) return Promise.resolve();
  state.bench.loading = true;

  function showUploadFallback() {
    state.bench.loading = false;
    const loadingEl = document.getElementById('b-loading');
    if (loadingEl) loadingEl.textContent = 'Could not auto-load. Please upload:';
  }

  if (location.protocol === 'file:') { showUploadFallback(); return Promise.resolve(); }

  return fetch('astral-bench.csv')
    .then(r => { if (r.ok) return r.text(); throw new Error(r.status); })
    .then(text => {
      const data = parseCSV(text);
      state.bench.loading = false;
      if (data.length > 0) {
        loadLocalDataset('bench', data);
      } else {
        showUploadFallback();
      }
    })
    .catch(() => { showUploadFallback(); });
}
loadBenchCSV();

// ── Transform Failures ───────────────────────────────────────────
async function loadTfailData() {
  if (state.tfail.loading || state.tfail.localData) return;
  state.tfail.loading = true;
  const card = document.getElementById('tf-card');
  if (card) card.innerHTML = '<div class="card-placeholder"><div class="loading-spinner">Loading...</div></div>';
  try {
    const resp = await fetch('consensus_failures_compact.json');
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    state.tfail.localData = data;
    state.tfail._filtered = data;
    state.tfail.total = data.length;
    state.tfail.idx = 0;
    const totalEl = document.getElementById('tf-total');
    if (totalEl) totalEl.textContent = data.length.toLocaleString();
    if (data.length > 0) renderItemFromData('tfail', data[0]);
    updateControls('tfail');
  } catch (e) {
    if (card) card.innerHTML = `<div class="card-placeholder"><p style="color:var(--red,#e74c3c)">Failed to load consensus_failures_compact.json: ${escapeHtml(e.message)}</p></div>`;
  } finally {
    state.tfail.loading = false;
  }
}

function updateTfailFilter() {
  const cat = state.tfail.filterCategory;
  const countEl = document.getElementById('tf-filter-count');

  // Local file mode: rebuild filtered array
  if (state.tfail.localData) {
    const filtered = cat ? state.tfail.localData.filter(r => r.failure_category === cat) : state.tfail.localData;
    if (countEl) countEl.textContent = cat ? `${filtered.length.toLocaleString()} matching` : '';
    state.tfail._filtered = filtered;
    state.tfail.total = filtered.length;
    state.tfail.idx = 0;
    if (filtered.length > 0) {
      renderItemFromData('tfail', filtered[0]);
    } else {
      const card = document.getElementById('tf-card');
      if (card) card.innerHTML = '<div class="card-placeholder"><p>No records match this filter.</p></div>';
    }
    updateControls('tfail');
    return;
  }

  // HF streaming mode: mark filter active and navigate from start
  if (countEl) countEl.textContent = cat ? '(filtered — skipping non-matching rows)' : '';
  state.tfail.idx = 0;
  updateControls('tfail');
  if (state.tfail.hfSplit) {
    goToIndex('tfail', 0);
  }
}

document.getElementById('tf-filter-category').addEventListener('change', (e) => {
  state.tfail.filterCategory = e.target.value;
  updateTfailFilter();
});

// ── Filter & Stats tracking ──────────────────────────────────────
function trackItemStats(key, item) {
  if (!item || !statsTracker[key]) return;
  const t = statsTracker[key];
  if (item.model) t.models[item.model] = (t.models[item.model] || 0) + 1;
  if (item.source) t.sources[item.source] = (t.sources[item.source] || 0) + 1;
  updateFilterOptions(key);
  updateMiniStats(key);
}

function updateFilterOptions(key) {
  const prefix = getPrefix(key);
  const t = statsTracker[key];

  const modelSelect = document.getElementById(`${prefix}-filter-model`);
  const sourceSelect = document.getElementById(`${prefix}-filter-source`);

  // Preserve current selection
  const curModel = modelSelect.value;
  const curSource = sourceSelect.value;

  // Update model options
  const models = Object.keys(t.models).sort();
  const modelOpts = '<option value="">All Models</option>' + models.map(m => `<option value="${escapeHtml(m)}">${escapeHtml(m)}</option>`).join('');
  if (modelSelect.innerHTML !== modelOpts) {
    modelSelect.innerHTML = modelOpts;
    modelSelect.value = curModel;
  }

  // Update source options
  const sources = Object.keys(t.sources).sort();
  const sourceOpts = '<option value="">All Sources</option>' + sources.map(s => `<option value="${escapeHtml(s)}">${escapeHtml(s)}</option>`).join('');
  if (sourceSelect.innerHTML !== sourceOpts) {
    sourceSelect.innerHTML = sourceOpts;
    sourceSelect.value = curSource;
  }
}

function updateMiniStats(key) {
  const prefix = getPrefix(key);
  const t = statsTracker[key];
  const container = document.getElementById(`${prefix}-mini-stats`);

  let html = '';
  // Model stats (only show models we've actually seen)
  const models = Object.entries(t.models).filter(([, c]) => c > 0).sort((a, b) => b[1] - a[1]);
  for (const [name, count] of models) {
    const short = name.replace(/^.*\//, ''); // strip org prefix
    html += `<span class="mini-stat">${escapeHtml(short)} <span class="mini-stat-val">${count}</span></span>`;
  }
  container.innerHTML = html;
}

function getActiveFilter(key) {
  const prefix = getPrefix(key);
  const modelEl = document.getElementById(`${prefix}-filter-model`);
  const sourceEl = document.getElementById(`${prefix}-filter-source`);
  return {
    model: modelEl ? modelEl.value : '',
    source: sourceEl ? sourceEl.value : '',
  };
}

function itemMatchesFilter(key, item) {
  if (!item) return true;
  if (key === 'tfail') {
    if (state.tfail.filterCategory && item.failure_category !== state.tfail.filterCategory) return false;
    return true;
  }
  const f = getActiveFilter(key);
  if (f.model && item.model !== f.model) return false;
  if (f.source && item.source !== f.source) return false;
  return true;
}

// Filtered navigation: find next matching item in given direction
async function goToFilteredIndex(key, direction) {
  const s = state[key];
  // Determine if any filter is active for this page type
  let hasFilter;
  if (key === 'tfail') {
    hasFilter = !!state.tfail.filterCategory;
  } else {
    const f = getActiveFilter(key);
    hasFilter = !!(f.model || f.source);
  }
  if (!hasFilter) {
    // No filter active, normal nav
    goToIndex(key, s.idx + direction);
    return;
  }

  const prefix = getPrefix(key);
  let checked = 0;
  let idx = s.idx + direction;

  const startIdx = s.idx;
  while (checked < 100) {
    idx = ((idx % s.total) + s.total) % s.total;
    if (checked > 0 && idx === startIdx) break; // wrapped all the way around
    // Try to get item from cache or fetch
    let item = s.cache.get(idx) || (s.localData ? s.localData[idx] : null);
    if (!item && s.hfSplit) {
      // Fetch batch
      try {
        const batchStart = idx;
        const batchLen = Math.min(BATCH_SIZE, s.total - batchStart);
        const result = await hfFetchRows(s.hfConfig, s.hfSplit, batchStart, batchLen, getHFDataset(key));
        if (result.rows) {
          for (let i = 0; i < result.rows.length; i++) {
            s.cache.set(batchStart + i, result.rows[i].row);
          }
        }
        item = s.cache.get(idx);
      } catch (e) { break; }
    }

    if (item && itemMatchesFilter(key, item)) {
      goToIndex(key, idx);
      return;
    }
    idx += direction;
    checked++;
  }

  // Update filter count to show no more matches
  const countEl = document.getElementById(`${prefix}-filter-count`);
  if (countEl) countEl.textContent = checked >= 100 ? '(no match in next 100)' : '(end of data)';
}

// Wire filter change events
['stage1', 'stage2', 'preview', 'error'].forEach(key => {
  const prefix = getPrefix(key);
  const modelSel = document.getElementById(`${prefix}-filter-model`);
  const sourceSel = document.getElementById(`${prefix}-filter-source`);
  const countEl = document.getElementById(`${prefix}-filter-count`);

  function onFilterChange() {
    const f = getActiveFilter(key);
    if (f.model || f.source) {
      countEl.textContent = '(filtered)';
    } else {
      countEl.textContent = '';
    }
  }

  modelSel.addEventListener('change', onFilterChange);
  sourceSel.addEventListener('change', onFilterChange);

  // Pre-populate dropdowns with known models
  updateFilterOptions(key);
});

// ── Keyboard navigation ─────────────────────────────────────────
document.addEventListener('keydown', (e) => {
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'SELECT') return;
  const raw = location.hash.replace('#', '') || 'overview';
  const page = raw.split('/')[0];
  if (page === 'overview') return;
  const key = page === 'bench' ? 'bench' : page;

  if (e.key === 'ArrowLeft' || e.key === 'a') {
    if (key !== 'bench') goToFilteredIndex(key, -1);
    else goToIndex(key, state[key].idx - 1);
  } else if (e.key === 'ArrowRight' || e.key === 'd') {
    if (key !== 'bench') goToFilteredIndex(key, 1);
    else goToIndex(key, state[key].idx + 1);
  }
});

// ── Image lightbox ───────────────────────────────────────────────
(function initLightbox() {
  const overlay = document.getElementById('lightbox');
  const lbImg = document.getElementById('lightbox-img');

  document.querySelectorAll('.image-card img').forEach(img => {
    img.addEventListener('click', () => {
      lbImg.src = img.src;
      lbImg.alt = img.alt;
      overlay.classList.add('active');
    });
  });

  overlay.addEventListener('click', () => overlay.classList.remove('active'));

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') overlay.classList.remove('active');
  });
})();

// ── Preview tab visibility ───────────────────────────────────────
// Hide the Preview tab and page when SHOW_PREVIEW_TAB is false.
(function applyPreviewVisibility() {
  if (!SHOW_PREVIEW_TAB) {
    const navLink = document.getElementById('nav-preview');
    if (navLink) navLink.style.display = 'none';
    const page = document.getElementById('page-preview');
    if (page) page.style.display = 'none';
  }
})();

// ── Judge panel drag-to-resize ───────────────────────────────────
// Allows users to drag the border between messages and judge panel
// to adjust the judge panel width. Syncs all panels on the page.
(function initJudgeResize() {
  let dragging = false;
  let startX = 0;
  let startWidth = 0;
  let activeHandle = null;

  document.addEventListener('mousedown', (e) => {
    const handle = e.target.closest('.judge-resize-handle');
    if (!handle) return;
    e.preventDefault();
    activeHandle = handle;
    activeHandle.classList.add('active');
    const panel = activeHandle.nextElementSibling;
    if (!panel || !panel.classList.contains('turn-judge-panel')) return;
    dragging = true;
    startX = e.clientX;
    startWidth = panel.offsetWidth;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
  });

  document.addEventListener('mousemove', (e) => {
    if (!dragging) return;
    // Dragging LEFT makes panel wider (negative delta = wider)
    const delta = startX - e.clientX;
    const newWidth = Math.max(200, Math.min(window.innerWidth * 0.6, startWidth + delta));
    // Apply to ALL judge panels on the page for consistent layout
    document.querySelectorAll('.turn-judge-panel').forEach(p => {
      p.style.width = newWidth + 'px';
    });
  });

  document.addEventListener('mouseup', () => {
    if (!dragging) return;
    dragging = false;
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
    if (activeHandle) {
      activeHandle.classList.remove('active');
      activeHandle = null;
    }
  });
})();
