import time
import requests
import math
from datetime import datetime, timezone

# ================= CONFIG =================

TG_TOKEN = "PASTE_YOUR_TELEGRAM_TOKEN"
TG_CHAT_ID = "PASTE_YOUR_CHAT_ID"

BASE_URL = "https://fapi.binance.com"

TOP_N_SYMBOLS = 60
MIN_VOLUME_USDT = 50_000_000  # фильтр ликвидности

D1_SWING_LEFT = 4
D1_SWING_RIGHT = 4

PIVOT_LEFT = 3
PIVOT_RIGHT = 3

D1_BLOCK_TOL_PCT = 0.002  # увеличили по твоему желанию
RETEST_TOL_PCT = 0.001

SLEEP_SECONDS = 60

# ================= TELEGRAM =================

def send_telegram(text):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    requests.post(url, json={
        "chat_id": TG_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True
    })

# ================= BINANCE =================

def get_top_symbols():
    r = requests.get(f"{BASE_URL}/fapi/v1/ticker/24hr")
    data = r.json()

    filtered = [
        x for x in data
        if x["symbol"].endswith("USDT")
        and float(x["quoteVolume"]) > MIN_VOLUME_USDT
    ]

    sorted_symbols = sorted(filtered, key=lambda x: float(x["quoteVolume"]), reverse=True)

    return [x["symbol"] for x in sorted_symbols[:TOP_N_SYMBOLS]]

def get_klines(symbol, interval, limit=200):
    r = requests.get(
        f"{BASE_URL}/fapi/v1/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit}
    )
    data = r.json()
    candles = []
    for c in data:
        candles.append({
            "ts": c[0],
            "open": float(c[1]),
            "high": float(c[2]),
            "low": float(c[3]),
            "close": float(c[4])
        })
    return candles

# ================= SWINGS =================

def compute_pivots(candles, left, right):
    highs = [False]*len(candles)
    lows = [False]*len(candles)

    for i in range(left, len(candles)-right):
        high = candles[i]["high"]
        low = candles[i]["low"]

        is_high = True
        is_low = True

        for j in range(i-left, i+right+1):
            if j == i:
                continue
            if candles[j]["high"] >= high:
                is_high = False
            if candles[j]["low"] <= low:
                is_low = False

        highs[i] = is_high
        lows[i] = is_low

    return highs, lows

# ================= D1 BLOCKS =================

def find_d1_blocks(d1):
    highs, lows = compute_pivots(d1, D1_SWING_LEFT, D1_SWING_RIGHT)

    blocks = []

    last_swing_high = None
    last_swing_low = None

    for i in range(len(d1)):
        if highs[i]:
            last_swing_high = d1[i]["high"]
        if lows[i]:
            last_swing_low = d1[i]["low"]

        c = d1[i]

        # Пробой вверх
        if last_swing_high and c["high"] > last_swing_high:
            for k in range(i-1, -1, -1):
                if d1[k]["close"] < d1[k]["open"]:
                    blocks.append({
                        "direction": "LONG",
                        "body_low": min(d1[k]["open"], d1[k]["close"]),
                        "body_high": max(d1[k]["open"], d1[k]["close"]),
                        "wick_low": d1[k]["low"],
                        "wick_high": d1[k]["high"],
                        "type": "ДВИЖУЩИЙ"
                    })
                    break

        # Пробой вниз
        if last_swing_low and c["low"] < last_swing_low:
            for k in range(i-1, -1, -1):
                if d1[k]["close"] > d1[k]["open"]:
                    blocks.append({
                        "direction": "SHORT",
                        "body_low": min(d1[k]["open"], d1[k]["close"]),
                        "body_high": max(d1[k]["open"], d1[k]["close"]),
                        "wick_low": d1[k]["low"],
                        "wick_high": d1[k]["high"],
                        "type": "ДВИЖУЩИЙ"
                    })
                    break

    return blocks

# ================= TOUCH =================

def is_touch(price_low, price_high, block):
    tol = D1_BLOCK_TOL_PCT

    zone_low = block["wick_low"] * (1 - tol)
    zone_high = block["wick_high"] * (1 + tol)

    return max(price_low, zone_low) <= min(price_high, zone_high)

# ================= STRUCTURE =================

def detect_structure(h1, direction):
    p1 = None
    p2 = None

    highs, lows = compute_pivots(h1, PIVOT_LEFT, PIVOT_RIGHT)

    for i in range(len(h1)):
        c = h1[i]

        if direction == "SHORT":
            if not p1 or c["high"] > p1:
                p1 = c["high"]
                p2 = None

            if p1 and not p2 and lows[i]:
                p2 = c["low"]

            if p2 and c["close"] < p2:
                return True

        if direction == "LONG":
            if not p1 or c["low"] < p1:
                p1 = c["low"]
                p2 = None

            if p1 and not p2 and highs[i]:
                p2 = c["high"]

            if p2 and c["close"] > p2:
                return True

    return False

# ================= MAIN LOOP =================

def main():
    send_telegram("✅ Бот запущен (Binance Futures)")

    while True:
        try:
            symbols = get_top_symbols()

            for symbol in symbols:
                d1 = get_klines(symbol, "1d", 200)
                blocks = find_d1_blocks(d1)
                if not blocks:
                    continue

                h1 = get_klines(symbol, "1h", 200)

                for block in blocks:
                    if not is_touch(h1[-1]["low"], h1[-1]["high"], block):
                        continue

                    if detect_structure(h1, block["direction"]):
                        emoji = "🟢" if block["direction"] == "LONG" else "🔴"
                        msg = f"{emoji} {block['direction']} | {symbol}\nТип блока: {block['type']}"
                        send_telegram(msg)

        except Exception as e:
            send_telegram(f"⚠️ Ошибка: {str(e)}")

        time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main()
