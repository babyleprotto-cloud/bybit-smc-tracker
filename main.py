/**
 * SMC Telegram Bot (Binance Futures USDT Perpetual)
 * Режим "рыболовная сеть" включён: больше сигналов, больше шума, потом ужесточим.
 *
 * Запуск:
 *  npm i node-fetch
 *  BOT_TOKEN=... CHAT_ID=... node bot.js
 *
 * Важно:
 * - Все тексты в Telegram на русском, тикер латиницей.
 * - Формат заголовка: "SOLUSDT  |  🔴 ШОРТ" / "BTCUSDT  |  🟢 ЛОНГ"
 */

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const fetch = require("node-fetch");

// ========================
// Конфиг "рыболовная сеть"
// ========================
const CFG = {
  // Binance Futures base
  BINANCE_FAPI: "https://fapi.binance.com",

  // Telegram
  BOT_TOKEN: process.env.BOT_TOKEN || "",
  CHAT_ID: process.env.CHAT_ID || "409865672", // можно переопределить env'ом

  // Сканирование
  TOP_N_SYMBOLS: 120,
  MIN_QUOTE_VOLUME_USDT: 1_000_000,
  SYMBOLS_PER_TICK: 40,
  TICK_INTERVAL_MS: 25_000,

  // D1 блоки
  D1_LOOKBACK: 220,
  D1_PIVOT_LEFT: 2,
  D1_PIVOT_RIGHT: 2,
  D1_BLOCK_TOL_PCT: 0.006,
  MAX_BLOCKS_PER_SYMBOL: 2, // 1 primary + 1 mitigation

  // H1 структура
  H1_LOOKBACK: 200,
  H1_PIVOT_LEFT: 1,
  H1_PIVOT_RIGHT: 1,
  RETEST_TOL_PCT: 0.005,

  // BOS
  BOS_MODE: "close_or_wick", // "close_only" или "close_or_wick"
  BOS_MIN_PCT: 0.0,
  BOS_WICK_TOL_PCT: 0.0015,

  // Антиспам (ослабленный)
  SIGNAL_TTL_HOURS: 12,

  // Диагностика
  DEBUG_PHASE_NOTIFICATIONS: false,

  // Heartbeat: 24/7 раз в час, не зависит от перезапуска
  HEARTBEAT_EVERY_MS: 60 * 60 * 1000,

  // Лимиты запросов
  HTTP_TIMEOUT_MS: 12_000,
  HTTP_MIN_GAP_MS: 220,
};

// =========
// Utilities
// =========
function nowMs() {
  return Date.now();
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function pctTol(price, pct) {
  return price * pct;
}
function clamp(n, a, b) {
  return Math.max(a, Math.min(b, n));
}
function fmt(n) {
  if (n == null || Number.isNaN(n)) return "—";
  const abs = Math.abs(n);
  if (abs >= 1000) return n.toFixed(2);
  if (abs >= 10) return n.toFixed(3);
  if (abs >= 1) return n.toFixed(4);
  return n.toFixed(6);
}
function isBull(c) {
  return c.close > c.open;
}
function isBear(c) {
  return c.close < c.open;
}

const STATE_PATH = path.join(__dirname, "state.json");
function loadState() {
  try {
    const raw = fs.readFileSync(STATE_PATH, "utf8");
    const s = JSON.parse(raw);
    return {
      rr_index: s.rr_index || 0,
      symbols_state: s.symbols_state || {},
      sent: s.sent || {},
      metrics: s.metrics || {},
      last_heartbeat_at_ms: s.last_heartbeat_at_ms || 0,
      last_cfg_hash: s.last_cfg_hash || "",
    };
  } catch {
    return {
      rr_index: 0,
      symbols_state: {},
      sent: {},
      metrics: {},
      last_heartbeat_at_ms: 0,
      last_cfg_hash: "",
    };
  }
}
function saveState(state) {
  fs.writeFileSync(STATE_PATH, JSON.stringify(state, null, 2));
}

// =====================
// Простенький rate-limit
// =====================
let lastHttpAt = 0;
async function httpGetJson(url) {
  const gap = nowMs() - lastHttpAt;
  if (gap < CFG.HTTP_MIN_GAP_MS) await sleep(CFG.HTTP_MIN_GAP_MS - gap);
  lastHttpAt = nowMs();

  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), CFG.HTTP_TIMEOUT_MS);
  try {
    const res = await fetch(url, { signal: ctrl.signal });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status}: ${text.slice(0, 160)}`);
    }
    return await res.json();
  } finally {
    clearTimeout(t);
  }
}

// =====================
// Binance data functions
// =====================
async function fetchExchangeInfo() {
  return httpGetJson(`${CFG.BINANCE_FAPI}/fapi/v1/exchangeInfo`);
}
async function fetch24hTickers() {
  return httpGetJson(`${CFG.BINANCE_FAPI}/fapi/v1/ticker/24hr`);
}
async function fetchKlines(symbol, interval, limit) {
  const url =
    `${CFG.BINANCE_FAPI}/fapi/v1/klines?symbol=${encodeURIComponent(symbol)}` +
    `&interval=${encodeURIComponent(interval)}&limit=${encodeURIComponent(String(limit))}`;
  const data = await httpGetJson(url);
  return data.map((k) => ({
    openTime: k[0],
    open: Number(k[1]),
    high: Number(k[2]),
    low: Number(k[3]),
    close: Number(k[4]),
    volume: Number(k[5]),
    closeTime: k[6],
    quoteVolume: Number(k[7]),
  }));
}

async function getTopSymbolsUSDTPerp() {
  const [ex, tickers] = await Promise.all([fetchExchangeInfo(), fetch24hTickers()]);

  const perpSet = new Set(
    (ex.symbols || [])
      .filter((s) => s.contractType === "PERPETUAL" && s.quoteAsset === "USDT" && s.status === "TRADING")
      .map((s) => s.symbol)
  );

  const filtered = (tickers || [])
    .filter((t) => perpSet.has(t.symbol))
    .map((t) => ({ symbol: t.symbol, quoteVolume: Number(t.quoteVolume) }))
    .filter((x) => Number.isFinite(x.quoteVolume) && x.quoteVolume >= CFG.MIN_QUOTE_VOLUME_USDT)
    .sort((a, b) => b.quoteVolume - a.quoteVolume)
    .slice(0, CFG.TOP_N_SYMBOLS);

  return filtered.map((x) => x.symbol);
}

// =========================
// Pivot helpers (фракталы)
// =========================
function computePivots(candles, left, right) {
  const pivots = [];
  for (let i = left; i < candles.length - right; i++) {
    const c = candles[i];
    let isHigh = true;
    let isLow = true;

    for (let j = i - left; j <= i + right; j++) {
      if (j === i) continue;
      if (candles[j].high >= c.high) isHigh = false;
      if (candles[j].low <= c.low) isLow = false;
      if (!isHigh && !isLow) break;
    }

    if (isHigh) pivots.push({ i, type: "high", price: c.high, time: c.openTime });
    if (isLow) pivots.push({ i, type: "low", price: c.low, time: c.openTime });
  }
  return pivots;
}

function lastPivotBefore(pivots, idx, type) {
  for (let k = pivots.length - 1; k >= 0; k--) {
    if (pivots[k].i < idx && pivots[k].type === type) return pivots[k];
  }
  return null;
}

// =========================
// D1 blocks (эвристика)
// =========================
function detectD1Blocks(d1Candles) {
  const pivots = computePivots(d1Candles, CFG.D1_PIVOT_LEFT, CFG.D1_PIVOT_RIGHT);
  const blocks = [];

  for (let i = CFG.D1_PIVOT_LEFT + 2; i < d1Candles.length; i++) {
    const c = d1Candles[i];

    const prevHigh = lastPivotBefore(pivots, i, "high");
    if (prevHigh && c.high > prevHigh.price) {
      const b = d1Candles[i - 1];
      if (isBear(b)) {
        blocks.push({
          id: `D1P_LONG_${b.openTime}`,
          side: "long",
          type: "Движущий",
          low: b.low,
          high: b.high,
          blockTime: b.openTime,
          brokenSwing: prevHigh.price,
        });
      }
    }

    const prevLow = lastPivotBefore(pivots, i, "low");
    if (prevLow && c.low < prevLow.price) {
      const b = d1Candles[i - 1];
      if (isBull(b)) {
        blocks.push({
          id: `D1P_SHORT_${b.openTime}`,
          side: "short",
          type: "Движущий",
          low: b.low,
          high: b.high,
          blockTime: b.openTime,
          brokenSwing: prevLow.price,
        });
      }
    }
  }

  blocks.sort((a, b) => b.blockTime - a.blockTime);
  const primary = blocks[0] ? [blocks[0]] : [];

  const mitigation = [];
  if (primary[0]) {
    const p = primary[0];
    const swing = p.brokenSwing;
    const afterIdx = d1Candles.findIndex((x) => x.openTime === p.blockTime);
    if (afterIdx >= 0) {
      for (let i = afterIdx + 1; i < d1Candles.length; i++) {
        const c = d1Candles[i];
        const tol = pctTol(swing, 0.0015);
        const touched = c.low <= swing + tol && c.high >= swing - tol;
        if (touched) {
          mitigation.push({
            id: `D1M_${p.side.toUpperCase()}_${c.openTime}`,
            side: p.side,
            type: "Смягчающий",
            low: Math.min(c.low, swing),
            high: Math.max(c.high, swing),
            blockTime: c.openTime,
            brokenSwing: swing,
          });
          break;
        }
      }
    }
  }

  return [...primary, ...mitigation].slice(0, CFG.MAX_BLOCKS_PER_SYMBOL);
}

// =========================
// H1: касание блока
// =========================
function checkTouchH1(block, h1Candle) {
  const tolLow = block.low * (1 - CFG.D1_BLOCK_TOL_PCT);
  const tolHigh = block.high * (1 + CFG.D1_BLOCK_TOL_PCT);

  const bodyLow = Math.min(h1Candle.open, h1Candle.close);
  const bodyHigh = Math.max(h1Candle.open, h1Candle.close);

  const wickTouch = h1Candle.low <= tolHigh && h1Candle.high >= tolLow;
  const bodyTouch = bodyLow <= tolHigh && bodyHigh >= tolLow;

  if (!wickTouch && !bodyTouch) return null;
  if (bodyTouch) return "ТЕЛО";
  return "ТЕНЬ";
}

// =========================
// H1 структура: P1 динамика, P2/P3 pivots, BOS, ретест
// =========================
function updateStructure(symbolState, h1Candles) {
  const touchTime = symbolState.touch_time;
  const fromIdx = h1Candles.findIndex((c) => c.openTime >= touchTime);
  const slice = fromIdx >= 0 ? h1Candles.slice(fromIdx) : h1Candles;

  if (slice.length < 10) return;

  const side = symbolState.side;

  // 1) Динамическая P1
  if (side === "short") {
    let maxH = -Infinity;
    let maxT = null;
    for (const c of slice) {
      if (c.high > maxH) {
        maxH = c.high;
        maxT = c.openTime;
      }
    }
    const prevP1 = symbolState.p1?.price ?? null;
    if (prevP1 == null || maxH > prevP1 + 1e-12) {
      symbolState.p1 = { price: maxH, time: maxT };
      symbolState.p2 = null;
      symbolState.p3 = null;
      symbolState.bos = null;
      symbolState.phase = "WAIT_BOS";
    }
  } else {
    let minL = Infinity;
    let minT = null;
    for (const c of slice) {
      if (c.low < minL) {
        minL = c.low;
        minT = c.openTime;
      }
    }
    const prevP1 = symbolState.p1?.price ?? null;
    if (prevP1 == null || minL < prevP1 - 1e-12) {
      symbolState.p1 = { price: minL, time: minT };
      symbolState.p2 = null;
      symbolState.p3 = null;
      symbolState.bos = null;
      symbolState.phase = "WAIT_BOS";
    }
  }

  // 2) Pivot'ы на H1 для P2/P3
  const pivots = computePivots(slice, CFG.H1_PIVOT_LEFT, CFG.H1_PIVOT_RIGHT);

  const p1Time = symbolState.p1?.time;
  const pivAfterP1 = p1Time ? pivots.filter((p) => slice[p.i].openTime >= p1Time) : pivots;

  if (!symbolState.p2) {
    if (side === "short") {
      const p2 = pivAfterP1.find((p) => p.type === "low");
      if (p2) symbolState.p2 = { price: p2.price, time: slice[p2.i].openTime };
    } else {
      const p2 = pivAfterP1.find((p) => p.type === "high");
      if (p2) symbolState.p2 = { price: p2.price, time: slice[p2.i].openTime };
    }
  }

  if (symbolState.p2 && !symbolState.p3) {
    const p2Time = symbolState.p2.time;
    const pivAfterP2 = pivAfterP1.filter((p) => slice[p.i].openTime > p2Time);
    if (side === "short") {
      const p3 = pivAfterP2.find((p) => p.type === "high");
      if (p3) symbolState.p3 = { price: p3.price, time: slice[p3.i].openTime };
    } else {
      const p3 = pivAfterP2.find((p) => p.type === "low");
      if (p3) symbolState.p3 = { price: p3.price, time: slice[p3.i].openTime };
    }
  }

  // 3) BOS
  if (symbolState.p2 && !symbolState.bos) {
    const level = symbolState.p2.price;
    const last = slice[slice.length - 1];

    const closeBreak =
      side === "short"
        ? last.close < level * (1 - CFG.BOS_MIN_PCT)
        : last.close > level * (1 + CFG.BOS_MIN_PCT);

    let wickBreak = false;
    if (CFG.BOS_MODE === "close_or_wick") {
      const tol = pctTol(level, CFG.BOS_WICK_TOL_PCT);
      wickBreak = side === "short" ? last.low < level - tol : last.high > level + tol;
    }

    if (closeBreak || wickBreak) {
      symbolState.bos = {
        level,
        time: last.openTime,
        close: last.close,
        mode: closeBreak ? "подтверждён закрытием" : "прокол уровня (по тени)",
      };
      symbolState.phase = "WAIT_RETEST";
    }
  }

  // 4) Ретест
  if (symbolState.phase === "WAIT_RETEST" && symbolState.bos) {
    const last = slice[slice.length - 1];
    const levels = [];
    if (symbolState.p2) levels.push({ name: "P2", price: symbolState.p2.price });
    if (symbolState.p3) levels.push({ name: "P3", price: symbolState.p3.price });

    for (const lv of levels) {
      const tol = pctTol(lv.price, CFG.RETEST_TOL_PCT);
      const touched = last.low <= lv.price + tol && last.high >= lv.price - tol;
      if (touched) {
        symbolState.retest = { levelName: lv.name, time: last.openTime };
        symbolState.phase = "SIGNAL_READY";
        break;
      }
    }
  }
}

// =========================
// Telegram
// =========================
async function sendTelegram(text) {
  if (!CFG.BOT_TOKEN || !CFG.CHAT_ID) throw new Error("Не задан BOT_TOKEN или CHAT_ID");
  const url = `https://api.telegram.org/bot${CFG.BOT_TOKEN}/sendMessage`;
  const body = { chat_id: CFG.CHAT_ID, text };
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`Telegram error ${res.status}: ${t.slice(0, 200)}`);
  }
}

function directionHeader(symbol, side) {
  return side === "long" ? `${symbol}  |  🟢 ЛОНГ` : `${symbol}  |  🔴 ШОРТ`;
}

function formatSignalMessage(symbol, st) {
  const lines = [];
  lines.push(directionHeader(symbol, st.side));
  lines.push("");
  lines.push(`Тип D1 блока: ${st.block_type}`);
  lines.push(`Диапазон D1: ${fmt(st.block_low)} – ${fmt(st.block_high)}`);
  lines.push("");
  lines.push(`Касание блока: ${st.touch_kind}`);
  lines.push(`P1: ${fmt(st.p1?.price)}`);
  lines.push(`P2: ${fmt(st.p2?.price)}`);
  lines.push(`P3: ${fmt(st.p3?.price)}`);
  lines.push("");
  if (st.bos) {
    const dirText = st.side === "long" ? "выше" : "ниже";
    lines.push(`Слом структуры (BOS): ${st.bos.mode}`);
    lines.push(`Уровень BOS (P2): ${fmt(st.bos.level)}`);
    lines.push(`Закрытие: ${fmt(st.bos.close)} (${dirText} уровня)`);
  } else {
    lines.push("Слом структуры (BOS): —");
  }
  lines.push(st.retest ? `Ретест: ${st.retest.levelName}` : "Ретест: —");
  return lines.join("\n");
}

function formatPhaseMessage(symbol, st) {
  const humanPhase =
    st.phase === "WAIT_D1_TOUCH"
      ? "Ожидание касания D1 блока"
      : st.phase === "WAIT_BOS"
      ? "Касание было, жду слом структуры"
      : st.phase === "WAIT_RETEST"
      ? "Слом был, жду ретест"
      : st.phase === "SIGNAL_READY"
      ? "Сигнал готов"
      : "—";
  return `${symbol}  |  ℹ️ ${humanPhase}`;
}

// =========================
// Антиспам
// =========================
function shouldSendByTtl(sentMap, symbol, touchId) {
  if (!sentMap[symbol]) return true;
  const ts = sentMap[symbol][touchId];
  if (!ts) return true;
  const ageH = (nowMs() - ts) / (1000 * 60 * 60);
  return ageH >= CFG.SIGNAL_TTL_HOURS;
}
function markSent(sentMap, symbol, touchId) {
  if (!sentMap[symbol]) sentMap[symbol] = {};
  sentMap[symbol][touchId] = nowMs();
}

// =========================
// Heartbeat + "настройки обновлены"
// =========================
function cfgPublicSnapshot() {
  // Только то, что реально важно видеть в чате
  return {
    TOP_N_SYMBOLS: CFG.TOP_N_SYMBOLS,
    MIN_QUOTE_VOLUME_USDT: CFG.MIN_QUOTE_VOLUME_USDT,
    SYMBOLS_PER_TICK: CFG.SYMBOLS_PER_TICK,
    D1_BLOCK_TOL_PCT: CFG.D1_BLOCK_TOL_PCT,
    H1_PIVOT_LEFT: CFG.H1_PIVOT_LEFT,
    H1_PIVOT_RIGHT: CFG.H1_PIVOT_RIGHT,
    BOS_MODE: CFG.BOS_MODE,
    BOS_WICK_TOL_PCT: CFG.BOS_WICK_TOL_PCT,
    RETEST_TOL_PCT: CFG.RETEST_TOL_PCT,
    SIGNAL_TTL_HOURS: CFG.SIGNAL_TTL_HOURS,
    HEARTBEAT_EVERY_MIN: Math.round(CFG.HEARTBEAT_EVERY_MS / 60000),
  };
}

function cfgHash(snapshot) {
  const json = JSON.stringify(snapshot);
  return crypto.createHash("sha256").update(json).digest("hex");
}

async function notifyIfCfgChanged(state) {
  const snap = cfgPublicSnapshot();
  const h = cfgHash(snap);
  if (h === state.last_cfg_hash) return;

  const lines = [];
  lines.push("⚙️ Настройки обновлены");
  for (const [k, v] of Object.entries(snap)) lines.push(`${k}: ${v}`);

  await sendTelegram(lines.join("\n"));
  state.last_cfg_hash = h;
}

function summarizePhases(state, symbols) {
  let a = 0,
    b = 0,
    c = 0,
    other = 0,
    r = 0;
  for (const s of symbols) {
    const st = state.symbols_state[s];
    const ph = st?.phase || "WAIT_D1_TOUCH";
    if (ph === "WAIT_D1_TOUCH") a++;
    else if (ph === "WAIT_BOS") b++;
    else if (ph === "WAIT_RETEST") c++;
    else if (ph === "SIGNAL_READY") r++;
    else other++;
  }
  return { a, b, c, other, r };
}

async function maybeHeartbeat(state, symbols) {
  const last = state.last_heartbeat_at_ms || 0;
  const due = nowMs() - last >= CFG.HEARTBEAT_EVERY_MS;

  if (!due) return;

  const { a, b, c, other, r } = summarizePhases(state, symbols);
  const m = state.metrics || {};
  const dt = new Date().toISOString().replace("T", " ").slice(0, 19);

  const lines = [];
  lines.push(`💓 Хартбит (${dt} UTC)`);
  lines.push(`Инструментов: ${symbols.length}`);
  lines.push(`Фазы: касание ${a} | слом ${b} | ретест ${c} | сигнал готов ${r} | прочее ${other}`);
  lines.push(`За сутки: касаний ${m.touches || 0} | BOS ${m.bos || 0} | ретестов ${m.retests || 0} | сигналов ${m.signals || 0}`);
  if (m.last_error) lines.push(`Ошибка: ${m.last_error}`);

  await sendTelegram(lines.join("\n"));
  state.last_heartbeat_at_ms = nowMs();
}

// =========================
// Core loop
// =========================
async function processSymbol(state, symbol) {
  const symState = state.symbols_state[symbol] || { phase: "WAIT_D1_TOUCH" };

  const [d1, h1] = await Promise.all([fetchKlines(symbol, "1d", CFG.D1_LOOKBACK), fetchKlines(symbol, "1h", CFG.H1_LOOKBACK)]);
  if (!d1.length || !h1.length) return;

  const blocks = detectD1Blocks(d1);
  if (!blocks.length) {
    symState.phase = "WAIT_D1_TOUCH";
    state.symbols_state[symbol] = symState;
    return;
  }

  // A) ждём касание
  if (symState.phase === "WAIT_D1_TOUCH") {
    const lastH1 = h1[h1.length - 1];

    for (const b of blocks) {
      const touchKind = checkTouchH1(b, lastH1);
      if (touchKind) {
        const touchId = `${b.id}:${lastH1.openTime}`;

        symState.phase = "WAIT_BOS";
        symState.side = b.side;
        symState.touch_id = touchId;
        symState.touch_time = lastH1.openTime;
        symState.touch_kind = touchKind;

        symState.block_id = b.id;
        symState.block_type = b.type;
        symState.block_low = b.low;
        symState.block_high = b.high;

        symState.p1 = null;
        symState.p2 = null;
        symState.p3 = null;
        symState.bos = null;
        symState.retest = null;

        state.metrics.touches = (state.metrics.touches || 0) + 1;

        if (CFG.DEBUG_PHASE_NOTIFICATIONS) {
          await sendTelegram(formatPhaseMessage(symbol, symState));
        }
        break;
      }
    }
  }

  // B/C) структура + BOS + ретест
  if (symState.phase === "WAIT_BOS" || symState.phase === "WAIT_RETEST") {
    const prevBosTime = symState.bos?.time || null;

    updateStructure(symState, h1);

    if (symState.bos && symState.bos.time !== prevBosTime) {
      state.metrics.bos = (state.metrics.bos || 0) + 1;
      if (CFG.DEBUG_PHASE_NOTIFICATIONS) {
        await sendTelegram(formatPhaseMessage(symbol, symState));
      }
    }

    if (symState.retest && symState.phase === "SIGNAL_READY") {
      state.metrics.retests = (state.metrics.retests || 0) + 1;
    }
  }

  // SIGNAL_READY) антиспам + отправка
  if (symState.phase === "SIGNAL_READY") {
    const touchId = symState.touch_id;
    if (touchId && shouldSendByTtl(state.sent, symbol, touchId)) {
      const msg = formatSignalMessage(symbol, symState);
      await sendTelegram(msg);
      markSent(state.sent, symbol, touchId);
      state.metrics.signals = (state.metrics.signals || 0) + 1;
    }
    symState.phase = "WAIT_D1_TOUCH";
  }

  state.symbols_state[symbol] = symState;
}

async function main() {
  if (!CFG.BOT_TOKEN) {
    console.log("Ошибка: не задан BOT_TOKEN. Пример: BOT_TOKEN=123:ABC CHAT_ID=... node bot.js");
    process.exit(1);
  }

  const state = loadState();
  if (!state.metrics) state.metrics = {};

  let symbols = [];
  try {
    symbols = await getTopSymbolsUSDTPerp();
    console.log(`Загружено символов: ${symbols.length}`);
  } catch (e) {
    console.error("Не удалось получить список символов:", e);
    process.exit(1);
  }

  // 1) Уведомление, что настройки изменились (после деплоя/изменения CFG)
  try {
    await notifyIfCfgChanged(state);
    saveState(state);
  } catch (e) {
    state.metrics.last_error = `Настройки: ${String(e.message || e).slice(0, 180)}`;
    saveState(state);
  }

  // 2) Если хартбита ещё не было вообще, можно сразу дать первый пинг
  if (!state.last_heartbeat_at_ms) {
    try {
      await maybeHeartbeat(state, symbols);
      saveState(state);
    } catch (e) {
      state.metrics.last_error = `Хартбит старт: ${String(e.message || e).slice(0, 180)}`;
      saveState(state);
    }
  }

  while (true) {
    try {
      // Хартбит по таймеру (24/7, независимо от минуты, с памятью в state.json)
      try {
        await maybeHeartbeat(state, symbols);
      } catch (e) {
        state.metrics.last_error = `Хартбит: ${String(e.message || e).slice(0, 180)}`;
      }

      const n = symbols.length;
      const batchSize = clamp(CFG.SYMBOLS_PER_TICK, 1, n || 1);
      const start = state.rr_index % (n || 1);
      const batch = [];
      for (let k = 0; k < batchSize; k++) batch.push(symbols[(start + k) % n]);
      state.rr_index = (start + batchSize) % (n || 1);

      for (const s of batch) {
        try {
          await processSymbol(state, s);
        } catch (e) {
          state.metrics.last_error = `${s}: ${String(e.message || e).slice(0, 180)}`;
        }
      }

      saveState(state);
    } catch (e) {
      state.metrics.last_error = `Цикл: ${String(e.message || e).slice(0, 180)}`;
      saveState(state);
    }

    await sleep(CFG.TICK_INTERVAL_MS);
  }
}

main().catch((e) => {
  console.error("Фатальная ошибка:", e);
  process.exit(1);
});