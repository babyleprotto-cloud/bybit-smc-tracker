#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import math
import traceback
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests


# =========================
# CONFIG
# =========================

CONFIG = {
    # Telegram
    "TG_TOKEN": os.environ.get("TG_TOKEN", "").strip(),
    "TG_CHAT_ID": os.environ.get("TG_CHAT_ID", "").strip(),

    # Binance Futures base
    "BINANCE_FAPI_BASE": os.environ.get("BINANCE_FAPI_BASE", "https://fapi.binance.com").strip(),

    # Universe
    "TOP_N_SYMBOLS": int(os.environ.get("TOP_N_SYMBOLS", "60")),
    "MIN_QUOTE_VOL_24H": float(os.environ.get("MIN_QUOTE_VOL_24H", "3000000")),  # USDT quoteVolume
    "SYMBOLS_STATIC": os.environ.get("SYMBOLS_STATIC", "").strip(),  # e.g. "BTCUSDT,ETHUSDT" (if non-empty, overrides top list)

    # Scheduler / load
    "RUN_EVERY_SECONDS": int(os.environ.get("RUN_EVERY_SECONDS", "60")),  # main loop tick
    "SYMBOLS_PER_TICK": int(os.environ.get("SYMBOLS_PER_TICK", "12")),    # round-robin batch size

    # Caches
    "UNIVERSE_CACHE_SEC": int(os.environ.get("UNIVERSE_CACHE_SEC", "600")),  # 10 min
    "KLINES_CACHE_SEC": int(os.environ.get("KLINES_CACHE_SEC", "60")),       # 1 min (H1 often enough)

    # D1 blocks search
    "D1_LOOKBACK_DAYS": int(os.environ.get("D1_LOOKBACK_DAYS", "90")),
    "D1_PIVOT_LEFT": int(os.environ.get("D1_PIVOT_LEFT", "2")),
    "D1_PIVOT_RIGHT": int(os.environ.get("D1_PIVOT_RIGHT", "2")),

    # H1 structure
    "H1_LOOKBACK_BARS": int(os.environ.get("H1_LOOKBACK_BARS", "260")),
    "H1_PIVOT_LEFT": int(os.environ.get("H1_PIVOT_LEFT", "3")),
    "H1_PIVOT_RIGHT": int(os.environ.get("H1_PIVOT_RIGHT", "3")),

    # Touch tolerance
    "D1_BLOCK_TOL_PCT": float(os.environ.get("D1_BLOCK_TOL_PCT", "0.0025")),  # 0.25% default

    # BOS rule (твоя правка: слом только по CLOSE за уровнем)
    # БЕЗ "каменного" буфера по умолчанию (0). Можно чуть-чуть поставить, если захочешь.
    "BOS_MIN_PCT": float(os.environ.get("BOS_MIN_PCT", "0.0")),  # 0.0 = ровно за уровень

    # Retest tolerance
    "RETEST_TOL_PCT": float(os.environ.get("RETEST_TOL_PCT", "0.0020")),

    # Heartbeat window (MSK)
    "HEARTBEAT_START_HOUR_MSK": int(os.environ.get("HEARTBEAT_START_HOUR_MSK", "10")),
    "HEARTBEAT_END_HOUR_MSK": int(os.environ.get("HEARTBEAT_END_HOUR_MSK", "22")),
}

MSK = timezone(timedelta(hours=3))

STATE_PATH = os.environ.get("SMC_STATE_PATH", "/opt/bybit-smc-tracker/state.json")  # оставляем путь как у тебя
HTTP_TIMEOUT = 12


# =========================
# Utilities
# =========================

def now_msk() -> datetime:
    return datetime.now(tz=MSK)

def ts_to_msk_str(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(MSK)
    return dt.strftime("%Y-%m-%d %H:%M")

def fmt_price(x: float) -> str:
    if x == 0:
        return "0"
    ax = abs(x)
    if ax >= 1000:
        return f"{x:.2f}".rstrip("0").rstrip(".")
    if ax >= 1:
        return f"{x:.4f}".rstrip("0").rstrip(".")
    return f"{x:.8f}".rstrip("0").rstrip(".")

def clamp(a: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, a))

def pct_tol(price: float, pct: float) -> float:
    return abs(price) * pct

def in_range(x: float, lo: float, hi: float, tol: float = 0.0) -> bool:
    a = min(lo, hi) - tol
    b = max(lo, hi) + tol
    return a <= x <= b

def ranges_intersect(a_lo: float, a_hi: float, b_lo: float, b_hi: float) -> bool:
    a1, a2 = min(a_lo, a_hi), max(a_lo, a_hi)
    b1, b2 = min(b_lo, b_hi), max(b_lo, b_hi)
    return not (a2 < b1 or b2 < a1)

def range_intersection(a_lo: float, a_hi: float, b_lo: float, b_hi: float) -> Optional[Tuple[float, float]]:
    if not ranges_intersect(a_lo, a_hi, b_lo, b_hi):
        return None
    return (max(min(a_lo, a_hi), min(b_lo, b_hi)), min(max(a_lo, a_hi), max(b_lo, b_hi)))

def is_bull(o: float, c: float) -> bool:
    return c > o

def is_bear(o: float, c: float) -> bool:
    return c < o


# =========================
# State (anti-spam / sessions)
# =========================

def _load_state() -> Dict[str, Any]:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}

def _save_state(st: Dict[str, Any]) -> None:
    tmp = STATE_PATH + ".tmp"
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False)
    os.replace(tmp, STATE_PATH)

def get_symbol_session_id(symbol: str) -> str:
    st = _load_state()
    return st.get("session", {}).get(symbol, "")

def set_symbol_session_id(symbol: str, session_id: str) -> None:
    st = _load_state()
    st.setdefault("session", {})[symbol] = session_id
    _save_state(st)

def was_sent(symbol: str, direction: str, session_id: str) -> bool:
    st = _load_state()
    key = f"{symbol}|{direction}|{session_id}"
    return bool(st.get("sent", {}).get(key, False))

def mark_sent(symbol: str, direction: str, session_id: str) -> None:
    st = _load_state()
    st.setdefault("sent", {})
    key = f"{symbol}|{direction}|{session_id}"
    st["sent"][key] = True

    # ограничим рост
    if len(st["sent"]) > 6000:
        items = list(st["sent"].items())[-4500:]
        st["sent"] = dict(items)

    _save_state(st)

def get_rr_index() -> int:
    st = _load_state()
    return int(st.get("rr_index", 0))

def set_rr_index(i: int) -> None:
    st = _load_state()
    st["rr_index"] = int(i)
    _save_state(st)

def get_last_heartbeat_hour_key() -> str:
    st = _load_state()
    return str(st.get("last_heartbeat_hour_key", ""))

def set_last_heartbeat_hour_key(k: str) -> None:
    st = _load_state()
    st["last_heartbeat_hour_key"] = k
    _save_state(st)


# =========================
# Telegram
# =========================

def send_telegram(text: str) -> None:
    token = CONFIG["TG_TOKEN"]
    chat_id = CONFIG["TG_CHAT_ID"]
    if not token or not chat_id:
        print("⚠️ TG_TOKEN / TG_CHAT_ID не заданы, сообщение не отправлено.")
        print(text)
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    r = requests.post(url, json=payload, timeout=HTTP_TIMEOUT)
    if not r.ok:
        raise RuntimeError(f"Telegram HTTP {r.status_code}: {r.text[:200]}")


# =========================
# Binance API
# =========================

_UNIVERSE_CACHE: Dict[str, Any] = {"ts": 0, "symbols": []}
_KLINES_CACHE: Dict[str, Any] = {}  # key -> {"ts":..., "data":...}

def http_get_json(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    r = requests.get(url, params=params or {}, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

def get_futures_universe() -> List[str]:
    static = CONFIG["SYMBOLS_STATIC"]
    if static:
        syms = [s.strip().upper() for s in static.split(",") if s.strip()]
        return list(dict.fromkeys(syms))

    now = time.time()
    if now - _UNIVERSE_CACHE["ts"] < CONFIG["UNIVERSE_CACHE_SEC"] and _UNIVERSE_CACHE["symbols"]:
        return _UNIVERSE_CACHE["symbols"]

    base = CONFIG["BINANCE_FAPI_BASE"].rstrip("/")
    # exchangeInfo: фильтр TRADING + perpetual
    ex = http_get_json(f"{base}/fapi/v1/exchangeInfo")
    allowed = set()
    for s in ex.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        if s.get("contractType") != "PERPETUAL":
            continue
        sym = s.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        allowed.add(sym)

    # 24hr tickers: quoteVolume сортировка
    tick = http_get_json(f"{base}/fapi/v1/ticker/24hr")
    rows = []
    for t in tick:
        sym = t.get("symbol", "")
        if sym not in allowed:
            continue
        try:
            qv = float(t.get("quoteVolume", "0") or "0")
        except Exception:
            qv = 0.0
        if qv < CONFIG["MIN_QUOTE_VOL_24H"]:
            continue
        rows.append((sym, qv))

    rows.sort(key=lambda x: x[1], reverse=True)
    syms = [s for s, _ in rows[: CONFIG["TOP_N_SYMBOLS"]]]

    _UNIVERSE_CACHE["ts"] = now
    _UNIVERSE_CACHE["symbols"] = syms
    return syms

def get_klines(symbol: str, interval: str, limit: int) -> List[Dict[str, Any]]:
    """
    Returns list of dicts:
    {ts, open, high, low, close}
    """
    base = CONFIG["BINANCE_FAPI_BASE"].rstrip("/")
    cache_key = f"{symbol}:{interval}:{limit}"
    now = time.time()
    c = _KLINES_CACHE.get(cache_key)
    if c and (now - c["ts"] < CONFIG["KLINES_CACHE_SEC"]):
        return c["data"]

    data = http_get_json(
        f"{base}/fapi/v1/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
    )
    out = []
    for k in data:
        out.append({
            "ts": int(k[0]),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
        })
    _KLINES_CACHE[cache_key] = {"ts": now, "data": out}
    return out


# =========================
# Pivot helpers
# =========================

def pivots_high(candles: List[Dict[str, Any]], left: int, right: int) -> List[Optional[float]]:
    n = len(candles)
    res: List[Optional[float]] = [None] * n
    for i in range(left, n - right):
        h = candles[i]["high"]
        ok = True
        for j in range(i - left, i):
            if candles[j]["high"] >= h:
                ok = False
                break
        if not ok:
            continue
        for j in range(i + 1, i + right + 1):
            if candles[j]["high"] > h:
                ok = False
                break
        if ok:
            res[i] = h
    return res

def pivots_low(candles: List[Dict[str, Any]], left: int, right: int) -> List[Optional[float]]:
    n = len(candles)
    res: List[Optional[float]] = [None] * n
    for i in range(left, n - right):
        l = candles[i]["low"]
        ok = True
        for j in range(i - left, i):
            if candles[j]["low"] <= l:
                ok = False
                break
        if not ok:
            continue
        for j in range(i + 1, i + right + 1):
            if candles[j]["low"] < l:
                ok = False
                break
        if ok:
            res[i] = l
    return res


# =========================
# D1 blocks (движущий / смягчающий)
# =========================

def candle_body_range(c: Dict[str, Any]) -> Tuple[float, float]:
    return (min(c["open"], c["close"]), max(c["open"], c["close"]))

def candle_wick_range(c: Dict[str, Any]) -> Tuple[float, float]:
    return (c["low"], c["high"])

def is_engulfing(prev: Dict[str, Any], curr: Dict[str, Any]) -> bool:
    """
    Поглощение по телу (как чаще всего рисуют):
    curr body полностью покрывает prev body.
    """
    p_lo, p_hi = candle_body_range(prev)
    c_lo, c_hi = candle_body_range(curr)
    return c_lo <= p_lo and c_hi >= p_hi and (c_hi - c_lo) > 0

def find_d1_blocks(symbol: str) -> List[Dict[str, Any]]:
    """
    Ищем кандидатов D1 блоков.

    Движущий блок (как ты описала):
    - свеча противоположного направления перед "импульсом", который обновил предыдущий swing high/low.
    - на практике: берём события пробоя D1 свинга и выбираем предыдущую свечу противоположного цвета.

    Смягчающий блок:
    - сидит около "перебитого" уровня (после пробоя), на уровне, который был сломан.
    - упрощённо: берём свечу, тело/тень которой перекрывает сломанный swing level (после пробоя).
    """
    d1 = get_klines(symbol, "1d", min(1000, max(60, CONFIG["D1_LOOKBACK_DAYS"] + 20)))
    if len(d1) < 20:
        return []

    ph = pivots_high(d1, CONFIG["D1_PIVOT_LEFT"], CONFIG["D1_PIVOT_RIGHT"])
    pl = pivots_low(d1, CONFIG["D1_PIVOT_LEFT"], CONFIG["D1_PIVOT_RIGHT"])

    swing_highs = [(i, ph[i]) for i in range(len(d1)) if ph[i] is not None]
    swing_lows = [(i, pl[i]) for i in range(len(d1)) if pl[i] is not None]

    blocks: List[Dict[str, Any]] = []

    # helper to find last swing before index
    def last_swing_before(swings: List[Tuple[int, float]], idx: int) -> Optional[Tuple[int, float]]:
        for i in range(len(swings) - 1, -1, -1):
            if swings[i][0] < idx:
                return swings[i]
        return None

    # scan for breakouts
    for i in range(5, len(d1)):
        c = d1[i]

        # breakout up: high breaks last swing high
        last_hi = last_swing_before(swing_highs, i)
        if last_hi and c["high"] > last_hi[1]:
            # moving block: previous bearish candle (opposite) before impulse
            j = i - 1
            while j >= 0 and not is_bear(d1[j]["open"], d1[j]["close"]):
                j -= 1
            if j >= 0:
                b = d1[j]
                blocks.append({
                    "type": "движущий",
                    "dir": "лонг",
                    "ts": b["ts"],
                    "body": candle_body_range(b),
                    "wick": candle_wick_range(b),
                    "meta": {"break_swing": last_hi[1], "break_ts": c["ts"]},
                })
            # mitigating block around broken swing level
            lvl = last_hi[1]
            # find first opposite candle after breakout that overlaps lvl
            k = i
            while k < len(d1):
                wk_lo, wk_hi = candle_wick_range(d1[k])
                if wk_lo <= lvl <= wk_hi:
                    bb = d1[k]
                    blocks.append({
                        "type": "смягчающий",
                        "dir": "лонг",
                        "ts": bb["ts"],
                        "body": candle_body_range(bb),
                        "wick": candle_wick_range(bb),
                        "meta": {"mitigate_level": lvl, "from_break_ts": c["ts"]},
                    })
                    break
                k += 1

        # breakout down: low breaks last swing low
        last_lo = last_swing_before(swing_lows, i)
        if last_lo and c["low"] < last_lo[1]:
            # moving block: previous bullish candle (opposite) before impulse
            j = i - 1
            while j >= 0 and not is_bull(d1[j]["open"], d1[j]["close"]):
                j -= 1
            if j >= 0:
                b = d1[j]
                blocks.append({
                    "type": "движущий",
                    "dir": "шорт",
                    "ts": b["ts"],
                    "body": candle_body_range(b),
                    "wick": candle_wick_range(b),
                    "meta": {"break_swing": last_lo[1], "break_ts": c["ts"]},
                })
            # mitigating block around broken swing level
            lvl = last_lo[1]
            k = i
            while k < len(d1):
                wk_lo, wk_hi = candle_wick_range(d1[k])
                if wk_lo <= lvl <= wk_hi:
                    bb = d1[k]
                    blocks.append({
                        "type": "смягчающий",
                        "dir": "шорт",
                        "ts": bb["ts"],
                        "body": candle_body_range(bb),
                        "wick": candle_wick_range(bb),
                        "meta": {"mitigate_level": lvl, "from_break_ts": c["ts"]},
                    })
                    break
                k += 1

    # оставим только последние (самые свежие) блоки каждого направления/типа
    # чтобы “не брать вообще из любого места”
    blocks.sort(key=lambda x: x["ts"], reverse=True)

    filtered: List[Dict[str, Any]] = []
    seen = set()
    for b in blocks:
        key = (b["type"], b["dir"])
        if key in seen:
            continue
        seen.add(key)
        filtered.append(b)

    return filtered


# =========================
# Touch logic (H1 touching D1 body/wick)
# =========================

def find_touch(symbol: str, d1_block: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Возвращает P1:
      - ts_h1 (свеча H1)
      - touch_price (факт касания)
      - touch_part: "ТЕЛО" / "ТЕНЬ"
      - h1_range: low-high
    """
    h1 = get_klines(symbol, "1h", CONFIG["H1_LOOKBACK_BARS"])
    if len(h1) < 10:
        return None

    tol = CONFIG["D1_BLOCK_TOL_PCT"]

    body_lo, body_hi = d1_block["body"]
    wick_lo, wick_hi = d1_block["wick"]

    # расширяем диапазоны блока на tolerance
    # толеранс считаем от середины диапазона блока
    mid = (wick_lo + wick_hi) / 2.0
    t = pct_tol(mid, tol)

    body_lo2, body_hi2 = body_lo - t, body_hi + t
    wick_lo2, wick_hi2 = wick_lo - t, wick_hi + t

    # ищем самое свежее касание (по последним закрытым свечам)
    # берем закрытые: последняя свеча может быть незакрыта, но в фьючах лучше брать предыдущую
    # => берём последние N-1
    for c in reversed(h1[:-1]):
        lo, hi = c["low"], c["high"]
        # касание тени блока (включает тело+тени)
        if ranges_intersect(lo, hi, wick_lo2, wick_hi2):
            # определим часть: тело или только тень
            part = "ТЕНЬ"
            if ranges_intersect(lo, hi, body_lo2, body_hi2):
                part = "ТЕЛО"
                inter = range_intersection(lo, hi, body_lo2, body_hi2)
            else:
                inter = range_intersection(lo, hi, wick_lo2, wick_hi2)
            touch_price = inter[0] if inter else clamp((lo + hi) / 2.0, wick_lo2, wick_hi2)
            return {
                "ts": c["ts"],
                "h1_low": lo,
                "h1_high": hi,
                "touch_price": touch_price,
                "touch_part": part,
            }
    return None


# =========================
# H1 structure (P1->P4)
# =========================

def detect_structure(symbol: str, direction: str, p1_ts: int) -> Optional[Dict[str, Any]]:
    """
    После P1 (касания) строим структуру на H1.
    Важно по твоим правилам:
      - Точка 1 динамическая: если появился новый более высокий хай (для шорта)
        или более низкий лой (для лонга) до слома — это новая Точка 1.
      - BOS только по CLOSE за уровнем (point2).
    """
    h1 = get_klines(symbol, "1h", CONFIG["H1_LOOKBACK_BARS"])
    if len(h1) < 30:
        return None

    # отрежем после P1
    idx0 = None
    for i, c in enumerate(h1):
        if c["ts"] >= p1_ts:
            idx0 = i
            break
    if idx0 is None:
        return None

    seq = h1[idx0:-1]  # только закрытые свечи
    if len(seq) < 20:
        return None

    ph = pivots_high(seq, CONFIG["H1_PIVOT_LEFT"], CONFIG["H1_PIVOT_RIGHT"])
    pl = pivots_low(seq, CONFIG["H1_PIVOT_LEFT"], CONFIG["H1_PIVOT_RIGHT"])

    # соберём список pivot-экстремумов по времени
    pivot_highs = [(i, ph[i], seq[i]["ts"]) for i in range(len(seq)) if ph[i] is not None]
    pivot_lows = [(i, pl[i], seq[i]["ts"]) for i in range(len(seq)) if pl[i] is not None]

    min_pct = CONFIG["BOS_MIN_PCT"]

    if direction == "шорт":
        # point1 = самый высокий pivot high после P1 (но он может обновляться)
        p1_i = None
        p2_i = None
        p3_i = None
        p4_ts = None

        p1_val = None
        p2_val = None
        p3_val = None

        # идём по времени и поддерживаем “самый высокий хай” как точку 1
        for i in range(len(seq)):
            # обновление точки 1
            if ph[i] is not None:
                if (p1_val is None) or (ph[i] > p1_val):
                    p1_val = ph[i]
                    p1_i = i
                    # как только точка 1 обновилась — всё ниже сбрасываем
                    p2_val = None; p2_i = None
                    p3_val = None; p3_i = None

            # точка 2: pivot low после точки 1
            if p1_i is not None and i > p1_i and pl[i] is not None:
                # берем первый подходящий pivot low как p2 (можно и минимум, но тогда сигнал будет позже)
                if p2_val is None:
                    p2_val = pl[i]; p2_i = i

            # точка 3: pivot high после p2, который ниже p1
            if p2_i is not None and i > p2_i and ph[i] is not None:
                if ph[i] < (p1_val if p1_val is not None else ph[i] + 1e9):
                    if p3_val is None:
                        p3_val = ph[i]; p3_i = i

            # BOS: close ниже p2 (только close)
            if p2_val is not None and i > (p3_i if p3_i is not None else p2_i):
                close_i = seq[i]["close"]
                eps = abs(p2_val) * min_pct
                if close_i < (p2_val - eps):
                    p4_ts = seq[i]["ts"]
                    return {
                        "direction": "шорт",
                        "p2": {"ts": seq[p2_i]["ts"], "price": p2_val},
                        "p3": {"ts": seq[p3_i]["ts"], "price": p3_val} if p3_i is not None else None,
                        "bos": {"ts": p4_ts, "close": close_i},
                    }

        return None

    else:  # "лонг"
        p1_i = None
        p2_i = None
        p3_i = None
        p4_ts = None

        p1_val = None
        p2_val = None
        p3_val = None

        for i in range(len(seq)):
            # обновление точки 1 (для лонга — самый низкий pivot low)
            if pl[i] is not None:
                if (p1_val is None) or (pl[i] < p1_val):
                    p1_val = pl[i]
                    p1_i = i
                    p2_val = None; p2_i = None
                    p3_val = None; p3_i = None

            # точка 2: pivot high после точки 1
            if p1_i is not None and i > p1_i and ph[i] is not None:
                if p2_val is None:
                    p2_val = ph[i]; p2_i = i

            # точка 3: pivot low после p2, который выше p1
            if p2_i is not None and i > p2_i and pl[i] is not None:
                if pl[i] > (p1_val if p1_val is not None else pl[i] - 1e9):
                    if p3_val is None:
                        p3_val = pl[i]; p3_i = i

            # BOS: close выше p2
            if p2_val is not None and i > (p3_i if p3_i is not None else p2_i):
                close_i = seq[i]["close"]
                eps = abs(p2_val) * min_pct
                if close_i > (p2_val + eps):
                    p4_ts = seq[i]["ts"]
                    return {
                        "direction": "лонг",
                        "p2": {"ts": seq[p2_i]["ts"], "price": p2_val},
                        "p3": {"ts": seq[p3_i]["ts"], "price": p3_val} if p3_i is not None else None,
                        "bos": {"ts": p4_ts, "close": close_i},
                    }

        return None


def detect_retest(symbol: str, direction: str, bos_ts: int, p2_price: float, p3_price: Optional[float]) -> Optional[Dict[str, Any]]:
    """
    После BOS ждём ретест P2 или P3 (касание H1 тенью/телом неважно, смотрим диапазон свечи).
    """
    h1 = get_klines(symbol, "1h", CONFIG["H1_LOOKBACK_BARS"])
    if len(h1) < 10:
        return None

    # берём свечи после bos_ts
    after = [c for c in h1[:-1] if c["ts"] > bos_ts]
    if not after:
        return None

    tol2 = pct_tol(p2_price, CONFIG["RETEST_TOL_PCT"])
    tol3 = pct_tol(p3_price, CONFIG["RETEST_TOL_PCT"]) if p3_price else None

    for c in after:
        lo, hi = c["low"], c["high"]
        if in_range(p2_price, lo, hi, tol2):
            return {"ts": c["ts"], "price": p2_price, "which": "P2"}
        if p3_price is not None and tol3 is not None and in_range(p3_price, lo, hi, tol3):
            return {"ts": c["ts"], "price": p3_price, "which": "P3"}
    return None


# =========================
# Messaging
# =========================

def direction_emoji(direction: str) -> str:
    return "🟢" if direction == "лонг" else "🔴"

def format_signal(symbol: str, d1_block: Dict[str, Any], p1: Dict[str, Any], st: Dict[str, Any], retest: Optional[Dict[str, Any]]) -> str:
    direction = st["direction"]
    emo = direction_emoji(direction)

    block_type = d1_block["type"].upper()  # ДВИЖУЩИЙ/СМЯГЧАЮЩИЙ
    d1_ts = ts_to_msk_str(d1_block["ts"])

    body_lo, body_hi = d1_block["body"]
    wick_lo, wick_hi = d1_block["wick"]

    lines = []
    lines.append(f"{emo} {direction.upper()} | {symbol}")
    lines.append(f"Тип блока: {block_type}")
    lines.append("")
    lines.append(f"D1 блок ({d1_ts}):")
    lines.append(f"Тело: {fmt_price(body_lo)}–{fmt_price(body_hi)} | Тень: {fmt_price(wick_lo)}–{fmt_price(wick_hi)}")
    lines.append("")
    lines.append(f"P1 (касание {p1['touch_part']}): {ts_to_msk_str(p1['ts'])} | H1 {fmt_price(p1['h1_low'])}–{fmt_price(p1['h1_high'])} | касание {fmt_price(p1['touch_price'])}")
    lines.append(f"P2: {ts_to_msk_str(st['p2']['ts'])} @ {fmt_price(st['p2']['price'])}")
    if st.get("p3") is not None:
        lines.append(f"P3: {ts_to_msk_str(st['p3']['ts'])} @ {fmt_price(st['p3']['price'])}")
    else:
        lines.append("P3: (не найден)")
    lines.append(f"Слом (H1): {ts_to_msk_str(st['bos']['ts'])} | close={fmt_price(st['bos']['close'])}")

    if retest is None:
        lines.append("P4: ждём ретест P2 или P3")
    else:
        lines.append(f"P4: {ts_to_msk_str(retest['ts'])} @ {fmt_price(retest['price'])} (ретест {retest['which']})")

    return "\n".join(lines)

def format_heartbeat(ts: datetime, scanned: int, total: int, c_touch: int, c_break: int, c_retest: int, errs: int) -> str:
    lines = []
    lines.append(f"🟢 Бот работает ({ts.strftime('%Y-%m-%d %H:%M')} МСК)")
    lines.append(f"Скан: {scanned}/{total} (round-robin)")
    lines.append("")
    lines.append(f"Ожидают касание D1: {c_touch}")
    lines.append(f"Коснулись D1, ждут слом H1: {c_break}")
    lines.append(f"Есть слом, ждут ретест (P2/P3): {c_retest}")
    lines.append("")
    lines.append(f"Ошибок в цикле: {errs}")
    return "\n".join(lines)


# =========================
# Core loop
# =========================

def should_send_heartbeat(ts: datetime) -> bool:
    h = ts.hour
    start = CONFIG["HEARTBEAT_START_HOUR_MSK"]
    end = CONFIG["HEARTBEAT_END_HOUR_MSK"]
    # окно [start, end)
    return start <= h < end

def heartbeat_key(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d %H")

def pick_round_robin(symbols: List[str]) -> List[str]:
    if not symbols:
        return []
    idx = get_rr_index()
    n = len(symbols)
    k = max(1, min(CONFIG["SYMBOLS_PER_TICK"], n))
    batch = []
    for _ in range(k):
        batch.append(symbols[idx % n])
        idx += 1
    set_rr_index(idx % n)
    return batch

def process_symbol(symbol: str) -> Tuple[str, Optional[str]]:
    """
    Returns:
      phase: "WAIT_TOUCH" | "WAIT_BREAK" | "WAIT_RETEST" | "DONE" | "ERR"
      error_str optional
    """
    try:
        blocks = find_d1_blocks(symbol)
        if not blocks:
            return ("WAIT_TOUCH", None)

        # выбираем блоки по направлению: для каждого блока будет свой сетап
        # но чтобы не спамить — берём самый свежий блок вообще
        d1_block = blocks[0]
        direction = d1_block["dir"]  # "лонг" / "шорт"

        # если D1 блок “поглотили” (по телу): считаем недействительным
        # упрощённо: если текущая D1 свеча телом перекрыла тело блока в обратную сторону
        # (это можно доработать позже, но хоть фильтр)
        d1 = get_klines(symbol, "1d", 5)
        if len(d1) >= 2:
            prev = d1[-2]
            curr = d1[-1]  # может быть незакрыта, но как фильтр ок
            if is_engulfing(prev, curr):
                # если текущее поглощение “на месте блока” — блок скипаем
                # (мягкий фильтр)
                return ("WAIT_TOUCH", None)

        p1 = find_touch(symbol, d1_block)
        if p1 is None:
            return ("WAIT_TOUCH", None)

        # фиксируем session_id на касание (B)
        session_id = str(p1["ts"])
        if get_symbol_session_id(symbol) != session_id:
            set_symbol_session_id(symbol, session_id)

        st = detect_structure(symbol, direction, p1["ts"])
        if st is None:
            return ("WAIT_BREAK", None)

        ret = detect_retest(
            symbol,
            direction,
            st["bos"]["ts"],
            st["p2"]["price"],
            st["p3"]["price"] if st.get("p3") else None,
        )

        # анти-спам: 1 сигнал на symbol+direction+session
        sid = get_symbol_session_id(symbol)
        if not sid:
            return ("WAIT_TOUCH", None)

        if not was_sent(symbol, direction, sid):
            msg = format_signal(symbol, d1_block, p1, st, ret)
            send_telegram(msg)
            mark_sent(symbol, direction, sid)

        if ret is None:
            return ("WAIT_RETEST", None)
        else:
            return ("DONE", None)

    except Exception as e:
        return ("ERR", f"{type(e).__name__}: {e}")


def main_loop() -> None:
    # стартовое сообщение один раз
    try:
        send_telegram("✅ Бот запущен (Binance Futures: D1 блок + разворот на H1)")
    except Exception:
        pass

    while True:
        ts = now_msk()
        errs = 0

        try:
            all_syms = get_futures_universe()
        except Exception:
            all_syms = []
            errs += 1

        batch = pick_round_robin(all_syms)

        c_touch = 0
        c_break = 0
        c_retest = 0
        c_done = 0

        for sym in batch:
            phase, err = process_symbol(sym)
            if phase == "WAIT_TOUCH":
                c_touch += 1
            elif phase == "WAIT_BREAK":
                c_break += 1
            elif phase == "WAIT_RETEST":
                c_retest += 1
            elif phase == "DONE":
                c_done += 1
            else:
                errs += 1
                if err:
                    # лог в stdout (journalctl)
                    print(f"ERR {sym}: {err}")

        # heartbeat: раз в час, только в окне
        hk = heartbeat_key(ts)
        last_hk = get_last_heartbeat_hour_key()
        if should_send_heartbeat(ts) and hk != last_hk:
            try:
                msg = format_heartbeat(ts, len(batch), len(all_syms), c_touch, c_break, c_retest, errs)
                send_telegram(msg)
                set_last_heartbeat_hour_key(hk)
            except Exception as e:
                print(f"Heartbeat send failed: {e}")

        # пауза
        time.sleep(max(5, CONFIG["RUN_EVERY_SECONDS"]))


if __name__ == "__main__":
    try:
        main_loop()
    except KeyboardInterrupt:
        print("Stopped.")
    except Exception:
        traceback.print_exc()
        raise