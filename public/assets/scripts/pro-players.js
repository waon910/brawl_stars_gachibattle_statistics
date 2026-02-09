const DATA_URL = '/data/map-meta/highest_rank_players.json';
const collator = new Intl.Collator('ja', { sensitivity: 'base', numeric: true });

const state = {
  players: [],
  filtered: [],
  sort: 'asc',
  view: 'card',
  loading: false,
};

const elements = {
  grid: document.getElementById('player-grid'),
  loading: document.getElementById('player-loading'),
  empty: document.getElementById('player-empty'),
  error: document.getElementById('player-error'),
  retry: document.getElementById('player-retry'),
  search: document.getElementById('player-search'),
  sort: document.getElementById('player-sort'),
  viewButtons: Array.from(document.querySelectorAll('.view-toggle button')),
  count: document.getElementById('player-count'),
};

function setLoading(isLoading) {
  if (!elements.loading) return;
  elements.loading.hidden = !isLoading;
  elements.loading.setAttribute('aria-hidden', String(!isLoading));
}

function setError(isError) {
  if (!elements.error) return;
  elements.error.hidden = !isError;
}

function setEmpty(isEmpty) {
  if (!elements.empty) return;
  elements.empty.hidden = !isEmpty;
}

function updateCount(total, shown) {
  if (!elements.count) return;
  if (total === 0) {
    elements.count.textContent = 'データなし';
    return;
  }
  const visible = shown ?? total;
  const percentage = Math.round((visible / total) * 100);
  elements.count.innerHTML = `<strong>${visible.toLocaleString('ja-JP')}</strong> / ${total.toLocaleString(
    'ja-JP',
  )} 人 (${percentage}%)`;
}

function normalizeText(value) {
  return value
    .toLocaleLowerCase('ja-JP')
    .normalize('NFKC')
    .replace(/\s+/g, '');
}

function getInitial(name) {
  const normalized = name.trim();
  if (!normalized) return '#';
  const first = normalized.charAt(0).toUpperCase();
  return first.match(/[A-Z0-9]/) ? first : normalized.charAt(0);
}

function getTone(name) {
  const hash = Array.from(name).reduce((acc, char) => acc + char.charCodeAt(0), 0);
  const tones = ['warm', 'cool', 'violet'];
  return tones[hash % tones.length];
}

function createCard(name, index) {
  const card = document.createElement('article');
  card.className = `pro-player-card${state.view === 'compact' ? ' compact' : ''}`;
  card.dataset.name = name;

  const meta = document.createElement('div');
  meta.className = 'pro-player-meta';

  const badge = document.createElement('div');
  badge.className = 'pro-player-badge';
  const tone = getTone(name);
  if (tone !== 'warm') {
    badge.dataset.tone = tone;
  }
  badge.textContent = getInitial(name);
  badge.setAttribute('aria-hidden', 'true');

  const rank = document.createElement('span');
  rank.className = 'pro-player-rank';
  rank.textContent = `No.${index + 1}`;

  meta.appendChild(badge);
  meta.appendChild(rank);

  const chip = document.createElement('span');
  chip.className = 'pro-player-chip';
  chip.textContent = 'PRO RANK';

  const heading = document.createElement('h3');
  heading.textContent = name;

  const description = document.createElement('p');
  description.textContent =
    'Brawl Stars ランクマッチでプロ帯に到達したプレイヤーです。さらなる高みを目指す旅は続きます。';

  const button = document.createElement('button');
  button.type = 'button';
  button.className = 'pro-player-copy';
  button.textContent = 'コピー';
  button.setAttribute('aria-label', `${name} をコピー`);
  button.addEventListener('click', () => copyName(name, button));

  card.append(meta, chip, heading, description, button);

  return card;
}

async function copyName(name, button) {
  const defaultText = 'コピー';
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(name);
    } else {
      await legacyCopy(name);
    }
    button.textContent = 'コピー済み';
  } catch (error) {
    console.error('コピーに失敗しました', error);
    button.textContent = 'コピー失敗';
  } finally {
    setTimeout(() => {
      button.textContent = defaultText;
    }, 2200);
  }
}

function legacyCopy(text) {
  return new Promise((resolve, reject) => {
    const textarea = document.createElement('textarea');
    textarea.value = text;
    textarea.setAttribute('readonly', '');
    textarea.style.position = 'absolute';
    textarea.style.left = '-9999px';
    document.body.appendChild(textarea);
    textarea.select();
    try {
      const result = document.execCommand('copy');
      if (!result) {
        throw new Error('execCommand failed');
      }
      resolve();
    } catch (error) {
      reject(error);
    } finally {
      document.body.removeChild(textarea);
    }
  });
}

function applyFilters() {
  const query = elements.search?.value ?? '';
  const trimmed = query.trim();
  const normalizedQuery = normalizeText(trimmed);

  let filtered = state.players.slice();

  if (normalizedQuery) {
    filtered = filtered.filter((name) => normalizeText(name).includes(normalizedQuery));
  }

  filtered.sort((a, b) => (state.sort === 'asc' ? collator.compare(a, b) : collator.compare(b, a)));

  state.filtered = filtered;
  renderPlayers();
}

function renderPlayers() {
  if (!elements.grid) return;

  if (state.loading) {
    return;
  }

  setLoading(false);
  setError(false);

  const total = state.players.length;
  const count = state.filtered.length;

  if (total === 0) {
    setEmpty(true);
    elements.grid.innerHTML = '';
    updateCount(0, 0);
    return;
  }

  if (count === 0) {
    setEmpty(true);
    elements.grid.innerHTML = '';
    updateCount(total, 0);
    return;
  }

  setEmpty(false);

  elements.grid.hidden = true;
  elements.grid.innerHTML = '';

  const fragment = document.createDocumentFragment();
  const chunkSize = state.view === 'compact' ? 120 : 60;
  let index = 0;

  function renderChunk() {
    const slice = state.filtered.slice(index, index + chunkSize);
    slice.forEach((name, offset) => {
      const card = createCard(name, index + offset);
      fragment.appendChild(card);
    });
    index += slice.length;
    if (index < state.filtered.length) {
      window.requestAnimationFrame(renderChunk);
    } else {
      elements.grid.appendChild(fragment);
      elements.grid.hidden = false;
      updateCount(total, state.filtered.length);
    }
  }

  window.requestAnimationFrame(renderChunk);
}

async function fetchPlayers() {
  state.loading = true;
  setLoading(true);
  setError(false);
  setEmpty(false);
  updateCount(0, 0);

  try {
    const response = await fetch(DATA_URL, { cache: 'no-store' });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    if (!Array.isArray(data)) {
      throw new Error('unexpected payload');
    }
    state.players = data.filter((value) => typeof value === 'string' && value.trim().length > 0);
    state.loading = false;
    applyFilters();
  } catch (error) {
    console.error('プロランクプレイヤーの取得に失敗しました', error);
    state.loading = false;
    setLoading(false);
    setEmpty(false);
    setError(true);
    if (elements.count) {
      elements.count.textContent = '読み込みエラー';
    }
  }
}

function setSort(value) {
  state.sort = value === 'desc' ? 'desc' : 'asc';
  applyFilters();
}

function setView(view) {
  state.view = view === 'compact' ? 'compact' : 'card';
  elements.viewButtons.forEach((button) => {
    const isActive = button.dataset.view === state.view;
    button.dataset.active = String(isActive);
    button.setAttribute('aria-pressed', String(isActive));
  });
  renderPlayers();
}

function setupEventListeners() {
  elements.search?.addEventListener('input', () => {
    window.requestAnimationFrame(applyFilters);
  });

  elements.sort?.addEventListener('change', (event) => {
    const target = event.target;
    if (!(target instanceof HTMLSelectElement)) return;
    setSort(target.value);
  });

  elements.viewButtons.forEach((button) => {
    button.addEventListener('click', () => {
      setView(button.dataset.view);
    });
  });

  elements.retry?.addEventListener('click', () => {
    fetchPlayers();
  });
}

function init() {
  setupEventListeners();
  fetchPlayers();
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}
