import os, time, json
import requests
from datetime import datetime, timezone

BYBIT_BASE = "https://api.bybit.com"
STATE_FILE = "state.json"

# ========= TELEGRAM =========
TG_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT = os.getenv("TG_CHAT_ID")

# ========= НАСТРОЙКИ =========
CATEGORIES = ["spot", "linear"]   # spot + фьючерсы
MIN_TURNOVER_24H = 5_000_000
TOP_N = 150
POLL_SEC = 60

LOOKBACK_DAYS_D1 = 45
D1_MODE = "body"  # body или range

PIVOT_LEFT = 2
PIVOT_RIGHT = 2

TOUCH_TOL = 0.0015
MOVE_AWAY = 0.003
COOLDOWN = 6 * 3600

# =================================

session = requests.Session()

def dir_ru(d):
    return "ЛОНГ 🟢" if d == "UP" else "ШОРТ 🔴"

def tg(msg):
    if not TG_TOKEN or not TG_CHAT:
        print(msg)
        return
    requests.post(
        f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
        json={"chat_id": TG_CHAT, "text": msg}
    )

def get(path, params):
    r = session.get(BYBIT_BASE + path, params=params)
    r.raise_for_status()
    data = r.json()
    if data["retCode"] != 0:
        raise Exception(data["retMsg"])
    return data["result"]

def human(ts):
    return datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")

def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except:
        return {}

def save_state(s):
    with open(STATE_FILE, "w") as f:
        json.dump(s, f)

def build_universe(cat):
    tickers = get("/v5/market/tickers", {"category": cat})["list"]
    rows = []
    for t in tickers:
        if not t["symbol"].endswith("USDT"):
            continue
        turnover = float(t.get("turnover24h", 0))
        if turnover < MIN_TURNOVER_24H:
            continue
        rows.append((t["symbol"], turnover, float(t["lastPrice"])))
    rows.sort(key=lambda x: x[1], reverse=True)
    return rows[:TOP_N]

def klines(cat, sym, interval, limit):
    rows = get("/v5/market/kline", {
        "category": cat,
        "symbol": sym,
        "interval": interval,
        "limit": limit
    })["list"]
    return sorted(rows, key=lambda r: int(r[0]))

def d1_blocks(cat, sym):
    rows = klines(cat, sym, "D", LOOKBACK_DAYS_D1+2)
    blocks = []
    for r in rows[:-1]:
        ts = int(r[0])
        o,h,l,c = map(float, r[1:5])
        if D1_MODE == "range":
            low, high = l,h
        else:
            low, high = min(o,c), max(o,c)
        blocks.append({"ts": ts, "low": low, "high": high})
    return blocks

def price_in_zone(p, low, high):
    tol = p * TOUCH_TOL
    return low - tol <= p <= high + tol

def pivots(rows):
    hi = [float(r[2]) for r in rows]
    lo = [float(r[3]) for r in rows]
    ts = [int(r[0]) for r in rows]
    highs, lows = [], []
    for i in range(PIVOT_LEFT, len(rows)-PIVOT_RIGHT):
        if hi[i] > max(hi[i-PIVOT_LEFT:i]) and hi[i] > max(hi[i+1:i+1+PIVOT_RIGHT]):
            highs.append((ts[i], hi[i]))
        if lo[i] < min(lo[i-PIVOT_LEFT:i]) and lo[i] < min(lo[i+1:i+1+PIVOT_RIGHT]):
            lows.append((ts[i], lo[i]))
    return highs, lows

def detect_break(highs, lows):
    if len(highs)<2 or len(lows)<2:
        return None
    (tL1,L1),(tL2,L2)=lows[-2],lows[-1]
    (tH1,H1),(tH2,H2)=highs[-2],highs[-1]
    if L2>L1 and H2>H1 and tL2<tH2:
        return {"dir":"UP","p2":H1,"p3":L2,"ts":tH2}
    if L2<L1 and H2<H1 and tH2<tL2:
        return {"dir":"DOWN","p2":L1,"p3":H2,"ts":tL2}
    return None

def near(p, lvl):
    return abs(p-lvl)<=p*TOUCH_TOL

def moved(p, lvl):
    return abs(p-lvl)>=p*MOVE_AWAY

# =============================

state = load_state()
tg("✅ Бот запущен (SMC D1 + H1)")

while True:
    try:
        for cat in CATEGORIES:
            universe = build_universe(cat)
            for sym, turnover, price in universe:
                key = f"{cat}:{sym}"
                st = state.get(key, {})

                blocks = d1_blocks(cat, sym)
                active = next((b for b in blocks if price_in_zone(price,b["low"],b["high"])), None)

                h1 = klines(cat, sym, "60", 200)
                highs,lows = pivots(h1)
                br = detect_break(highs,lows)

                if br and active:
                    if st.get("break_ts") != br["ts"]:
                        st = {
                            "break_ts": br["ts"],
                            "dir": br["dir"],
                            "p2": br["p2"],
                            "p3": br["p3"],
                            "armed_p2": False,
                            "armed_p3": False,
                            "alert": 0
                        }
                        tg(f"🧩 {sym} ({cat})\nСЛОМ: {dir_ru(br['dir'])}\n"
                           f"D1 блок: {active['low']} - {active['high']}\n"
                           f"P2={br['p2']} | P3={br['p3']}")

                if "p2" in st:
                    if not st["armed_p2"] and moved(price,st["p2"]):
                        st["armed_p2"]=True
                    if not st["armed_p3"] and moved(price,st["p3"]):
                        st["armed_p3"]=True

                    if time.time()-st.get("alert",0)>COOLDOWN:
                        if st["armed_p2"] and near(price,st["p2"]):
                            st["alert"]=time.time()
                            tg(f"🔔 {sym}\nРЕТЕСТ P2\n{dir_ru(st['dir'])}\nЦена: {price}")
                        if st["armed_p3"] and near(price,st["p3"]):
                            st["alert"]=time.time()
                            tg(f"🔔 {sym}\nРЕТЕСТ P3\n{dir_ru(st['dir'])}\nЦена: {price}")

                state[key]=st

        save_state(state)

    except Exception as e:
        tg(f"Ошибка: {e}")

    time.sleep(POLL_SEC)
