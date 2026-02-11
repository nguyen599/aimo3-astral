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
const HF_API = 'https://datasets-server.huggingface.co';
const BATCH_SIZE = 20;  // rows to prefetch per batch

// HF datasets-server uses config+split. We try multiple patterns
// since HF auto-generates these from filenames in different ways.
const HF_SPLITS = {
  stage1: [
    { config: 'default', split: 'train' },
  ],
  stage2: [
    { config: 'default', split: 'train_2' },
  ],
};

// ── State ────────────────────────────────────────────────────────
const state = {
  stage1: { cache: new Map(), idx: 0, total: 0, hfConfig: null, hfSplit: null, localData: null, loading: false },
  stage2: { cache: new Map(), idx: 0, total: 0, hfConfig: null, hfSplit: null, localData: null, loading: false },
  bench:  { cache: new Map(), idx: 0, total: 0, localData: null, loading: false },
};

// ── Routing ──────────────────────────────────────────────────────
function navigate() {
  const hash = location.hash.replace('#', '') || 'stage1';
  const page = ['stage1', 'stage2', 'bench'].includes(hash) ? hash : 'stage1';

  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));

  document.getElementById('page-' + page).classList.add('active');
  const link = document.querySelector(`.nav-link[data-page="${page}"]`);
  if (link) link.classList.add('active');

  // Auto-init HF streaming when navigating to a stage page
  if ((page === 'stage1' || page === 'stage2') && !state[page].localData && !state[page].hfSplit) {
    initHFStream(page);
  }
}
window.addEventListener('hashchange', navigate);
navigate();

// ── HuggingFace Datasets Server API ─────────────────────────────
async function hfFetchRows(config, split, offset, length) {
  const url = `${HF_API}/rows?dataset=${encodeURIComponent(HF_DATASET)}&config=${encodeURIComponent(config)}&split=${encodeURIComponent(split)}&offset=${offset}&length=${length}`;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`HF API ${resp.status}`);
  return resp.json();
}

async function initHFStream(key) {
  const s = state[key];
  if (s.hfSplit || s.localData || s.loading) return;
  s.loading = true;

  const prefix = key === 'stage1' ? 's1' : 's2';
  const card = document.getElementById(`${prefix}-card`);
  showCardLoading(card, `Connecting to HuggingFace...`);

  const candidates = HF_SPLITS[key] || [];
  let found = false;

  for (const { config, split } of candidates) {
    try {
      const result = await hfFetchRows(config, split, 0, 1);
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

  const prefix = key === 'bench' ? 'b' : (key === 'stage1' ? 's1' : 's2');

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
    const result = await hfFetchRows(s.hfConfig, s.hfSplit, batchStart, batchLen);

    if (result.rows) {
      for (let i = 0; i < result.rows.length; i++) {
        s.cache.set(batchStart + i, result.rows[i].row);
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
  hfFetchRows(s.hfConfig, s.hfSplit, startIdx, batchLen)
    .then(result => {
      if (result.rows) {
        for (let i = 0; i < result.rows.length; i++) {
          s.cache.set(startIdx + i, result.rows[i].row);
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
      readFile(e.target.files[0], prefix === 's1' ? 'stage1' : 'stage2', 'jsonl');
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

  const prefix = key === 'bench' ? 'b' : (key === 'stage1' ? 's1' : 's2');
  const totalEl = document.getElementById(`${prefix}-total`);
  if (totalEl) totalEl.textContent = data.length.toLocaleString();

  renderItemFromData(key, data[0]);
  updateControls(key);
}

// ── Rendering ────────────────────────────────────────────────────
function escapeHtml(str) {
  if (str == null) return '';
  return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
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
        if (typeof args === 'object') {
          args = args.code || JSON.stringify(args, null, 2);
        }
        html += `<div style="margin-top:0.3rem"><span style="color:#f5a623;font-size:0.75rem">${escapeHtml(fname)}()</span></div>`;
        html += `<div class="tool-code">${escapeHtml(args)}</div>`;
      }
    }

    // Content
    if (msg.content && !(typeof msg.content === 'string' && msg.content.trim() === '')) {
      const text = typeof msg.content === 'string' ? msg.content : JSON.stringify(msg.content, null, 2);
      html += `<div style="margin-top:0.2rem">${escapeHtml(text)}</div>`;
    }

    html += '</div>';
  }
  html += '</div>';
  return html;
}

function renderDatasetItem(item) {
  let html = '';

  // Meta pills
  const pills = [];
  if (item.model) pills.push(['Model', item.model]);
  if (item.source) pills.push(['Source', item.source]);
  if (item.category) pills.push(['Category', item.category]);
  if (item.success_rate != null) pills.push(['Success Rate', Number(item.success_rate).toFixed(3)]);
  if (item.attempts != null) pills.push(['Attempts', item.attempts]);
  if (item.tokens_len != null) pills.push(['Tokens', Number(item.tokens_len).toLocaleString()]);
  if (item.transform != null) pills.push(['Transformed', item.transform ? 'Yes' : 'No']);
  if (item.license) pills.push(['License', item.license]);

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

  // Answer
  if (item.answer != null) {
    html += '<div class="field-group">';
    html += '<div class="field-label">Answer</div>';
    html += `<div class="field-value answer-text">${escapeHtml(String(item.answer))}</div>`;
    html += '</div>';
  }

  // Messages
  if (item.messages) {
    html += '<div class="field-group">';
    html += '<div class="field-label">Messages / TIR Trace</div>';
    html += `<div class="field-value messages-field">${renderMessages(item.messages)}</div>`;
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

function renderItemFromData(key, item) {
  const prefix = key === 'bench' ? 'b' : (key === 'stage1' ? 's1' : 's2');
  const card = document.getElementById(`${prefix}-card`);
  if (!item) { card.innerHTML = '<div class="card-placeholder"><p>No data</p></div>'; return; }

  let html;
  if (key === 'bench') {
    html = renderBenchItem(item);
  } else {
    html = renderDatasetItem(item);
  }
  card.innerHTML = html;
  requestAnimationFrame(() => renderMath(card));
}

function updateControls(key) {
  const s = state[key];
  const prefix = key === 'bench' ? 'b' : (key === 'stage1' ? 's1' : 's2');

  document.getElementById(`${prefix}-counter`).textContent =
    s.total > 0 ? `${s.idx + 1} / ${s.total.toLocaleString()}` : '0 / 0';
  document.getElementById(`${prefix}-prev`).disabled = s.idx <= 0;
  document.getElementById(`${prefix}-next`).disabled = s.idx >= s.total - 1;
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

// ── Navigation helpers ───────────────────────────────────────────
function goToIndex(key, idx) {
  const s = state[key];
  if (idx < 0 || idx >= s.total) return;

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
  const prefix = key === 'bench' ? 'b' : (key === 'stage1' ? 's1' : 's2');

  document.getElementById(`${prefix}-prev`).addEventListener('click', () => {
    goToIndex(key, state[key].idx - 1);
  });
  document.getElementById(`${prefix}-next`).addEventListener('click', () => {
    goToIndex(key, state[key].idx + 1);
  });
  document.getElementById(`${prefix}-go`).addEventListener('click', () => {
    const input = document.getElementById(`${prefix}-jump`);
    const val = parseInt(input.value, 10);
    if (val >= 1 && val <= state[key].total) {
      goToIndex(key, val - 1);
    }
    input.value = '';
  });
  document.getElementById(`${prefix}-jump`).addEventListener('keydown', (e) => {
    if (e.key === 'Enter') document.getElementById(`${prefix}-go`).click();
  });
}
setupControls('stage1');
setupControls('stage2');
setupControls('bench');

// ── File inputs (fallback) ───────────────────────────────────────
document.getElementById('s1-file').addEventListener('change', (e) => {
  readFile(e.target.files[0], 'stage1', 'jsonl');
});
document.getElementById('s2-file').addEventListener('change', (e) => {
  readFile(e.target.files[0], 'stage2', 'jsonl');
});
document.getElementById('b-file').addEventListener('change', (e) => {
  readFile(e.target.files[0], 'bench', 'csv');
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
  } else if (name.includes('stage2') || name.includes('s2')) {
    readFile(file, 'stage2', 'jsonl');
    location.hash = 'stage2';
  } else {
    readFile(file, 'stage1', 'jsonl');
    location.hash = 'stage1';
  }
});

// ── Auto-load AstralBench CSV ────────────────────────────────────
(function autoLoadBench() {
  const loadingEl = document.getElementById('b-loading');

  function showUploadFallback() {
    if (loadingEl) loadingEl.textContent = 'Could not auto-load. Please upload:';
  }

  if (location.protocol === 'file:') { showUploadFallback(); return; }

  fetch('astral-bench.csv')
    .then(r => { if (r.ok) return r.text(); throw new Error(r.status); })
    .then(text => {
      const data = parseCSV(text);
      if (data.length > 0) {
        loadLocalDataset('bench', data);
      } else {
        showUploadFallback();
      }
    })
    .catch(() => { showUploadFallback(); });
})();

// ── Keyboard navigation ─────────────────────────────────────────
document.addEventListener('keydown', (e) => {
  if (e.target.tagName === 'INPUT') return;
  const hash = location.hash.replace('#', '') || 'stage1';
  const key = hash === 'bench' ? 'bench' : hash;

  if (e.key === 'ArrowLeft' || e.key === 'a') {
    goToIndex(key, state[key].idx - 1);
  } else if (e.key === 'ArrowRight' || e.key === 'd') {
    goToIndex(key, state[key].idx + 1);
  }
});
