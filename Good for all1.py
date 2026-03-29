import websocket
import json
import pandas as pd
import time
import threading
import queue
import requests
from datetime import datetime, timezone
import csv
import os

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =============================
# CONFIG
# =============================
PAIRS = ["BTCUSDT", "ETHUSDT", "XAUUSDT", "XAGUSDT"]
INTERVAL = 15
HIGHER_TF = 60
CHECK_INTERVAL = 5
COOLDOWN = 600

BOT_TOKEN = "8775610162:AAGu8DRI60TfsE4ngObg-Q6sZVvfd8bboiI"
CHAT_ID = "7434243701"

ACCOUNT_BALANCE = 1000
RISK_PER_TRADE = 0.02
MAX_DAILY_LOSS = 0.30

BOT_RUNNING = True
WS_CONNECTED = False
LAST_UPDATE_ID = None

daily_loss = 0
last_reset_day = datetime.now(timezone.utc).day

# =============================
# SESSION (ANTI-ERROR)
# =============================
session = requests.Session()
retry = Retry(total=3, backoff_factor=1,
              status_forcelist=[500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retry))

def clear_webhook():
    """
    Deletes any existing Telegram webhook to prevent 409 conflicts.
    """
    try:
        r = session.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook?drop_pending_updates=true",
            timeout=10
        )
        print("Webhook cleared:", r.json())
    except Exception as e:
        print("Failed to clear webhook:", e)

# Call this once at startup
clear_webhook()

# =============================
# STORAGE
# =============================
ws_queue = queue.Queue()
candles_data = {p: pd.DataFrame() for p in PAIRS}
last_signal_time = {p: 0 for p in PAIRS}
last_signal_message = {p: None for p in PAIRS}
last_candle_time = {p: 0 for p in PAIRS}

SIGNAL_FILE = "signals_log.csv"
if not os.path.exists(SIGNAL_FILE):
    with open(SIGNAL_FILE, "w", newline="") as f:
        csv.writer(f).writerow([
            "time","pair","direction","entry","sl","tp",
            "risk","size","score","confidence","sr_type","sr_price","taps"
        ])


# =============================
# TELEGRAM FUNCTIONS
# =============================
def send_telegram(msg):
    """
    Sends a message to Telegram with automatic retries and 409/404 handling.
    """
    for attempt in range(3):  # try up to 3 times
        try:
            r = session.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"},
                timeout=10
            )

            if r.status_code == 200:
                return  # message sent successfully

            elif r.status_code == 409:
                # Telegram webhook conflict: auto-delete webhook
                print("⚠️ Telegram 409: webhook conflict detected. Deleting webhook...")
                session.get(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=10)
                time.sleep(1)

            elif r.status_code == 404:
                print("⚠️ Telegram 404: bot not found. Check BOT_TOKEN and CHAT_ID.")
                return

            else:
                print(f"⚠️ Telegram unexpected status: {r.status_code}")

        except requests.exceptions.ConnectionError:
            print("⚠️ Telegram connection error. Retrying...")
            time.sleep(1)

        except requests.exceptions.Timeout:
            print("⚠️ Telegram timeout. Retrying...")
            time.sleep(1)

        except Exception as e:
            print("⚠️ Telegram unknown error:", e)
            time.sleep(1)


def check_commands():
    global BOT_RUNNING, LAST_UPDATE_ID

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    params = {"timeout": 10, "limit": 5}

    if LAST_UPDATE_ID:
        params["offset"] = LAST_UPDATE_ID + 1

    try:
        r = session.get(url, params=params, timeout=20)

        if r.status_code == 409:
            # Webhook exists: delete it and return
            print("⚠️ 409 conflict → deleting webhook")
            session.get(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=10)
            return

        if r.status_code != 200:
            print("Telegram bad response:", r.status_code)
            return

        data = r.json()

        for update in data.get("result", []):
            LAST_UPDATE_ID = update["update_id"]

            msg = update.get("message", {})
            text = msg.get("text", "").strip().lower()

            if text == "/start":
                BOT_RUNNING = True
                send_telegram("Bot started 🚀")

            elif text == "/stop":
                BOT_RUNNING = False
                send_telegram("Bot paused ⏸")

            elif text == "/status":
                send_telegram(f"Running: {BOT_RUNNING}\nWS: {WS_CONNECTED}")

            elif text == "/signal":
                if any(last_signal_message.values()):
                    for m in last_signal_message.values():
                        if m:
                            send_telegram(m)
                else:
                    send_telegram("No signals yet.")

    except Exception as e:
        print("Command error:", e)

# =============================
# TELEGRAM LISTENER THREAD
# =============================
def telegram_listener():
    while True:
        try:
            check_commands()
            time.sleep(2)
        except Exception as e:
            print("Telegram listener error:", e)
            time.sleep(2)

# =============================
# NEWS FILTER
# =============================
def high_impact_news():
    try:
        r = session.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json", timeout=10)
        data = r.json()
        now = datetime.now(timezone.utc)

        for e in data:
            if e.get("impact") != "High":
                continue

            t = datetime.fromisoformat(e["date"]).astimezone(timezone.utc)
            if abs((t - now).total_seconds()) < 1800:
                return True
    except:
        pass

    return False

# =============================
# DATA
# =============================
def load_history(pair):
    try:
        r = session.get(
            "https://api.bybit.com/v5/market/kline",
            params={"category":"linear","symbol":pair,"interval":str(INTERVAL),"limit":200},
            timeout=10
        )
        klines = r.json()["result"]["list"]
        df = pd.DataFrame([{
            "time":float(c[0]),
            "open":float(c[1]),
            "high":float(c[2]),
            "low":float(c[3]),
            "close":float(c[4]),
            "volume":float(c[5])
        } for c in klines]).sort_values("time")
        print(pair,"loaded")
        return df
    except:
        return pd.DataFrame()

# =============================
# INDICATORS
# =============================
def calculate_rsi(df, p=14):
    d = df["close"].diff()
    gain = d.clip(lower=0).rolling(p).mean()
    loss = -d.clip(upper=0).rolling(p).mean()
    rs = gain / loss
    return (100 - 100/(1+rs)).iloc[-1]

def calculate_atr(df, p=14):
    return (df["high"] - df["low"]).rolling(p).mean().iloc[-1]

# =============================
# MARKET STRUCTURE
# =============================
def market_structure(df):
    highs, lows = df["high"].values, df["low"].values
    sh, sl = [], []

    for i in range(2, len(df)-2):
        if highs[i] > highs[i-1] and highs[i] > highs[i+1]:
            sh.append(highs[i])
        if lows[i] < lows[i-1] and lows[i] < lows[i+1]:
            sl.append(lows[i])

    if len(sh) < 2 or len(sl) < 2:
        return None, None

    if sh[-1] > sh[-2] and sl[-1] > sl[-2]:
        return "UPTREND", "BULLISH_BOS"
    elif sh[-1] < sh[-2] and sl[-1] < sl[-2]:
        return "DOWNTREND", "BEARISH_BOS"

    return "RANGE", None

# =============================
# SMART LOGIC
# =============================
def detect_sr(df):
    highs, lows = df["high"].values, df["low"].values
    sh, sl = [], []

    for i in range(2, len(df)-2):
        if highs[i] > highs[i-1] and highs[i] > highs[i+1]:
            sh.append(highs[i])
        if lows[i] < lows[i-1] and lows[i] < lows[i+1]:
            sl.append(lows[i])

    if not sh or not sl:
        return None

    res, sup = max(sh), min(sl)
    taps_res = sum(abs(df["high"]-res)<res*0.002)
    taps_sup = sum(abs(df["low"]-sup)<sup*0.002)

    return {"price":res,"type":"RESISTANCE","taps":taps_res} if taps_res>taps_sup else {"price":sup,"type":"SUPPORT","taps":taps_sup}

def rejection_candle(last, direction):
    body = abs(last["close"] - last["open"])
    if direction == "BUY":
        return (min(last["open"],last["close"]) - last["low"]) > body*1.5
    if direction == "SELL":
        return (last["high"] - max(last["open"],last["close"])) > body*1.5
    return False

def breakout_retest(df, level, direction):
    last, prev = df.iloc[-1], df.iloc[-2]
    if direction == "BUY":
        return prev["close"] < level and last["close"] > level
    if direction == "SELL":
        return prev["close"] > level and last["close"] < level
    return False

def get_trend(pair):
    try:
        r = session.get("https://api.bybit.com/v5/market/kline",
                        params={"category":"linear","symbol":pair,"interval":"60","limit":100})
        closes = [float(c[4]) for c in r.json()["result"]["list"]]
        df = pd.DataFrame({"c":closes})
        return "BUY" if df["c"].ewm(span=20).mean().iloc[-1] > df["c"].ewm(span=50).mean().iloc[-1] else "SELL"
    except:
        return None

def score_pair(pair, df, sr, direction):
    score = 0
    score += min(sr["taps"], 3)

    trend = get_trend(pair)
    if trend == direction:
        score += 2

    _, bos = market_structure(df)
    if bos:
        score += 2

    vol = df["volume"].iloc[-1]
    avg = df["volume"].tail(20).mean()
    if vol > avg * 1.5:
        score += 2

    rsi = calculate_rsi(df)
    if direction=="BUY" and rsi<60:
        score += 1
    elif direction=="SELL" and rsi>40:
        score += 1

    return min(score,10)

# =============================
# DRAWDOWN
# =============================
def check_drawdown():
    global daily_loss, last_reset_day, BOT_RUNNING

    today = datetime.now(timezone.utc).day
    if today != last_reset_day:
        daily_loss = 0
        last_reset_day = today

    if daily_loss >= ACCOUNT_BALANCE * MAX_DAILY_LOSS:
        BOT_RUNNING = False
        send_telegram("🛑 Max daily loss reached.")
        return False

    return True

# =============================
# SIGNAL GENERATION (FIXED ELITE)
# =============================
def generate_signal(pair, df):
    global daily_loss

    if high_impact_news():
        return None

    if len(df) < 60:
        return None

    last, prev = df.iloc[-1], df.iloc[-2]

    # Prevent duplicate candle signals
    if last["time"] == last_candle_time[pair]:
        return None
    last_candle_time[pair] = last["time"]

    # Cooldown
    if time.time() - last_signal_time[pair] < COOLDOWN:
        return None

    atr = calculate_atr(df)

    # 🔥 RELAXED VOLATILITY FILTER
    if atr < df["close"].iloc[-1] * 0.0003:
        return None

    sr = detect_sr(df)
    if not sr:
        return None

    # 🔥 IMPROVED DIRECTION LOGIC
    trend = get_trend(pair)
    direction = trend if trend else ("BUY" if sr["type"] == "SUPPORT" else "SELL")

    structure, bos = market_structure(df)

    # 🔥 FLEXIBLE ENTRY (NOT TOO STRICT)
    breakout = breakout_retest(df, sr["price"], direction)
    rejection = rejection_candle(last, direction)

    if not breakout and not rejection:
        return None

    rsi = calculate_rsi(df)

    # RSI filter
    if direction == "BUY" and rsi > 75:
        return None
    if direction == "SELL" and rsi < 25:
        return None

    # Prices
    entry = round(last["close"], 2)
    sl = round(entry - atr * 1.5, 2) if direction == "BUY" else round(entry + atr * 1.5, 2)
    tp = round(entry + atr * 3, 2) if direction == "BUY" else round(entry - atr * 3, 2)

    score = score_pair(pair, df, sr, direction)
    confidence = int((score / 10) * 100)

    risk = ACCOUNT_BALANCE * RISK_PER_TRADE
    size = round(risk / abs(entry - sl), 2) if entry != sl else 0

    msg = f"""
🚨 TRADE SIGNAL {'🟢' if direction=='BUY' else '🔴'}
💎 {pair}
➡️ {direction}

💰 Entry: {entry}
🛑 Stop Loss: {sl}
🎯 Take Profit: {tp}

⭐ Score: {score}/10
💡 Confidence: {confidence}%
📊 SR: {sr['type']} ({sr['taps']} taps)
"""

    last_signal_time[pair] = time.time()
    last_signal_message[pair] = msg
    daily_loss += risk

    # Save
    with open(SIGNAL_FILE, "a", newline="") as f:
        csv.writer(f).writerow([
            datetime.now(timezone.utc), pair, direction,
            entry, sl, tp, RISK_PER_TRADE, size,
            score, confidence, sr["type"], sr["price"], sr["taps"]
        ])

    send_telegram(msg)
    return msg

# =============================
# WEBSOCKET
# =============================
def on_message(ws, message):
    try:
        msg = json.loads(message)
        if "data" not in msg:
            return
        ws_queue.put(msg)
    except:
        pass

def on_open(ws):
    global WS_CONNECTED
    WS_CONNECTED=True
    for p in PAIRS:
        ws.send(json.dumps({"op":"subscribe","args":[f"kline.{INTERVAL}.{p}"]}))

def on_close(ws, a, b):
    global WS_CONNECTED
    WS_CONNECTED=False

def start_ws():
    while True:
        try:
            ws = websocket.WebSocketApp(
                "wss://stream.bybit.com/v5/public/linear",
                on_message=on_message,
                on_open=on_open,
                on_close=on_close
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print("WS error:", e)
            time.sleep(5)

# =============================
# INIT
# =============================
for p in PAIRS:
    candles_data[p] = load_history(p)

threading.Thread(target=start_ws, daemon=True).start()
threading.Thread(target=telegram_listener, daemon=True).start()

print("🚀 ELITE BOT RUNNING")

# =============================
# MAIN LOOP
# =============================
while True:
    try:
        if not BOT_RUNNING or not check_drawdown():
            time.sleep(CHECK_INTERVAL)
            continue

        while not ws_queue.empty():
            msg = ws_queue.get()
            pair = msg.get("topic","").split(".")[-1]

            for c in msg.get("data", []):
                if not c.get("confirm"):
                    continue

                new = {
                    "time":float(c["start"]),
                    "open":float(c["open"]),
                    "high":float(c["high"]),
                    "low":float(c["low"]),
                    "close":float(c["close"]),
                    "volume":float(c["volume"])
                }

                df = candles_data[pair]
                candles_data[pair] = pd.concat([df, pd.DataFrame([new])]).iloc[-300:]

        for pair in PAIRS:
            df = candles_data[pair]
            if not df.empty:
                generate_signal(pair, df)

        time.sleep(CHECK_INTERVAL)

    except Exception as e:
        print("Main error:", e)
        time.sleep(3)
