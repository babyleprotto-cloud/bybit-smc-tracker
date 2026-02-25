#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import traceback
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests

MSK = timezone(timedelta(hours=3))

CONFIG = {
    "TG_TOKEN": os.environ.get("TG_TOKEN", "").strip(),
    "TG_CHAT_ID": os.environ.get("TG_CHAT_ID", "").strip(),
    "BINANCE_BASE": "https://fapi.binance.com",

    "TOP_N_SYMBOLS": 60,
    "MIN_QUOTE_VOL_24H": 3000000,

    "RUN_EVERY_SECONDS": 60,

    "D1_PIVOT_LEFT": 3,
    "D1_PIVOT_RIGHT": 3,
    "H1_PIVOT_LEFT": 2,
    "H1_PIVOT_RIGHT": 2,

    "D1_BLOCK_TOL_PCT": 0.0035,
    "RETEST_TOL_PCT": 0.0030,
    "BOS_MIN_PCT": 0.0,

    "HEARTBEAT_START": 10,
    "HEARTBEAT_END": 22,
}

STATE_FILE = "state.json"
LAST_HEARTBEAT = None


# ================= TELEGRAM =================

def send_telegram(text: str):
    if not CONFIG["TG_TOKEN"] or not CONFIG["TG_CHAT_ID"]:
        print(text)
        return
    url = f"https://api.telegram.org/bot{CONFIG['TG_TOKEN']}/sendMessage"
    requests.post(url, json={
        "chat_id": CONFIG["TG_CHAT_ID"],
        "text": text,
        "disable_web_page_preview": True
    }, timeout=10)


# ================= STATE =================

def load_state():
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except:
        return {}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

def already_sent(symbol, session):
    state = load_state()
    return state.get(f"{symbol}_{session}", False)

def mark_sent(symbol, session):
    state = load_state()
    state[f"{symbol}_{session}"] = True
    save_state(state)


# ================= BINANCE =================

def get_top_symbols():
    r = requests.get(f"{CONFIG['BINANCE_BASE']}/fapi/v1/ticker/24hr", timeout=10)
    data = r.json()
    pairs = []
    for row in data:
        if not row["symbol"].endswith("USDT"):
            continue
        qv = float(row["quoteVolume"])
        if qv >= CONFIG["MIN_QUOTE_VOL_24H"]:
            pairs.append((row["symbol"], qv))
    pairs.sort(key=lambda x: x[1], reverse=True)
    return [p[0] for p in pairs[:CONFIG["TOP_N_SYMBOLS"]]]


def get_klines(symbol, interval, limit):
    r = requests.get(
        f"{CONFIG['BINANCE_BASE']}/fapi/v1/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=10
    )
    data = r.json()
    candles = []
    for k in data:
        candles.append({
            "ts": int(k[0]),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4])
        })
    return candles


# ================= HELPERS =================

def pivot_high(candles, i, left, right):
    h = candles[i]["high"]
    for j in range(i-left, i+right+1):
        if j == i: continue
        if candles[j]["high"] >= h:
            return False
    return True

def pivot_low(candles, i, left, right):
    l = candles[i]["low"]
    for j in range(i-left, i+right+1):
        if j == i: continue
        if candles[j]["low"] <= l:
            return False
    return True


# ================= LOGIC =================

def find_d1_block(symbol):
    d1 = get_klines(symbol, "1d", 200)
    for i in range(10, len(d1)-1):
        c = d1[i]
        if pivot_high(d1, i, CONFIG["D1_PIVOT_LEFT"], CONFIG["D1_PIVOT_RIGHT"]):
            if d1[i+1]["low"] < c["low"]:
                return {
                    "dir": "шорт",
                    "body": (min(c["open"], c["close"]), max(c["open"], c["close"])),
                    "wick": (c["low"], c["high"]),
                    "ts": c["ts"]
                }
        if pivot_low(d1, i, CONFIG["D1_PIVOT_LEFT"], CONFIG["D1_PIVOT_RIGHT"]):
            if d1[i+1]["high"] > c["high"]:
                return {
                    "dir": "лонг",
                    "body": (min(c["open"], c["close"]), max(c["open"], c["close"])),
                    "wick": (c["low"], c["high"]),
                    "ts": c["ts"]
                }
    return None


def check_touch(symbol, block):
    h1 = get_klines(symbol, "1h", 200)
    for c in reversed(h1[:-1]):
        if c["low"] <= block["wick"][1] and c["high"] >= block["wick"][0]:
            part = "ТЕЛО" if c["low"] <= block["body"][1] and c["high"] >= block["body"][0] else "ТЕНЬ"
            return {"ts": c["ts"], "part": part}
    return None


def detect_bos(symbol, direction, p1_ts):
    h1 = get_klines(symbol, "1h", 200)
    p1_index = None
    for i,c in enumerate(h1):
        if c["ts"] == p1_ts:
            p1_index = i
            break
    if p1_index is None:
        return None

    for i in range(p1_index+5, len(h1)-1):
        if direction == "шорт":
            if h1[i]["close"] < h1[i-1]["low"]:
                return {"ts": h1[i]["ts"], "price": h1[i]["close"]}
        else:
            if h1[i]["close"] > h1[i-1]["high"]:
                return {"ts": h1[i]["ts"], "price": h1[i]["close"]}
    return None


# ================= HEARTBEAT =================

def heartbeat():
    global LAST_HEARTBEAT
    now = datetime.now(MSK)
    if not (CONFIG["HEARTBEAT_START"] <= now.hour < CONFIG["HEARTBEAT_END"]):
        return
    if now.minute != 0:
        return
    key = now.strftime("%Y-%m-%d %H")
    if key == LAST_HEARTBEAT:
        return
    send_telegram(f"🟢 Бот работает ({now.strftime('%Y-%m-%d %H:%M')} МСК)")
    LAST_HEARTBEAT = key


# ================= MAIN =================

def main():
    send_telegram("✅ Бот запущен")

    while True:
        try:
            symbols = get_top_symbols()

            for symbol in symbols:
                block = find_d1_block(symbol)
                if not block:
                    continue

                touch = check_touch(symbol, block)
                if not touch:
                    continue

                session = str(touch["ts"])
                if already_sent(symbol, session):
                    continue

                bos = detect_bos(symbol, block["dir"], touch["ts"])
                if not bos:
                    continue

                emoji = "🔴" if block["dir"] == "шорт" else "🟢"

                msg = (
                    f"{emoji} {block['dir'].upper()} | {symbol}\n\n"
                    f"D1 блок: {datetime.fromtimestamp(block['ts']/1000, tz=MSK).strftime('%Y-%m-%d')}\n"
                    f"Касание: {touch['part']}\n"
                    f"Слом (CLOSE): {datetime.fromtimestamp(bos['ts']/1000, tz=MSK).strftime('%Y-%m-%d %H:%M')}"
                )

                send_telegram(msg)
                mark_sent(symbol, session)

            heartbeat()

        except Exception as e:
            print("ERROR:", e)
            traceback.print_exc()

        time.sleep(CONFIG["RUN_EVERY_SECONDS"])


if __name__ == "__main__":
    main()