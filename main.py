import os
import time
import threading
import requests
import yfinance as yf
import pandas as pd
from flask import Flask

# --- ระบบ Web Server สำหรับ Render ---
app = Flask(__name__)

@app.route('/')
def home():
    return "SMC Pro (24/7 + TP/SL) Bot is running!"

def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

# --- ข้อมูล Telegram Bot ---
TOKEN = "8977273894:AAEcJ-KSwZF7TZClxGaNauK76rGrzQ1I6D0"
CHAT_ID = "1484260985"

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        requests.post(url, json=payload)
    except Exception as e:
        print("Error sending message:", e)

last_signal_time = None

def check_trading_signal():
    global last_signal_time

    # ดึงข้อมูล Multi-Timeframe (H1 และ 15M)
    df_h1 = yf.download(tickers="GC=F", period="10d", interval="1h", progress=False)
    df_15m = yf.download(tickers="GC=F", period="5d", interval="15m", progress=False)

    if df_h1.empty or df_15m.empty or len(df_15m) < 205 or len(df_h1) < 200:
        return

    # จัดการ MultiIndex
    if isinstance(df_h1.columns, pd.MultiIndex):
        df_h1.columns = df_h1.columns.get_level_values(0)
    if isinstance(df_15m.columns, pd.MultiIndex):
        df_15m.columns = df_15m.columns.get_level_values(0)

    # คำนวณ EMA200 บน H1 (เทรนภาพใหญ่)
    df_h1['EMA200'] = df_h1['Close'].ewm(span=200, adjust=False).mean()
    h1_trend_up = df_h1['Close'].iloc[-1] > df_h1['EMA200'].iloc[-1]
    h1_trend_down = df_h1['Close'].iloc[-1] < df_h1['EMA200'].iloc[-1]

    # ดึงแท่งเทียน 15M
    c_prev2 = df_15m.iloc[-3]
    c_prev1 = df_15m.iloc[-2]
    c_curr  = df_15m.iloc[-1]
    
    curr_time = df_15m.index[-1]
    if last_signal_time == curr_time:
        return

    entry_price = c_curr['Close']

    # --- BUY SETUP (H1 Uptrend + 15M FVG) ---
    has_bullish_fvg = c_prev2['High'] < c_curr['Low']
    
    if h1_trend_up and has_bullish_fvg:
        fvg_top = c_curr['Low']
        fvg_bottom = c_prev2['High']
        
        sl_price = fvg_bottom - 1.5
        risk = entry_price - sl_price
        tp_price = entry_price + (risk * 2)

        msg = (
            f"🟢 **SIGNAL BUY (XAUUSD)**\n"
            f"🌐 **Timeframe:** 15M (Conform H1 Uptrend)\n\n"
            f"📥 **Entry Price:** `{entry_price:.2f}`\n"
            f"🎯 **Target (TP 1:2):** `{tp_price:.2f}`\n"
            f"🛡️ **Stop Loss (SL):** `{sl_price:.2f}`\n"
            f"📌 **FVG Zone:** `{fvg_bottom:.2f} - {fvg_top:.2f}`"
        )
        send_telegram(msg)
        last_signal_time = curr_time
        return

    # --- SELL SETUP (H1 Downtrend + 15M FVG) ---
    has_bearish_fvg = c_prev2['Low'] > c_curr['High']

    if h1_trend_down and has_bearish_fvg:
        fvg_top = c_prev2['Low']
        fvg_bottom = c_curr['High']
        
        sl_price = fvg_top + 1.5
        risk = sl_price - entry_price
        tp_price = entry_price - (risk * 2)

        msg = (
            f"🔴 **SIGNAL SELL (XAUUSD)**\n"
            f"🌐 **Timeframe:** 15M (Conform H1 Downtrend)\n\n"
            f"📥 **Entry Price:** `{entry_price:.2f}`\n"
            f"🎯 **Target (TP 1:2):** `{tp_price:.2f}`\n"
            f"🛡️ **Stop Loss (SL):** `{sl_price:.2f}`\n"
            f"📌 **FVG Zone:** `{fvg_bottom:.2f} - {fvg_top:.2f}`"
        )
        send_telegram(msg)
        last_signal_time = curr_time
        return

def bot_loop():
    send_telegram("🚀 อัปเดตบอท SMC Pro (รัน 24 ชม. + H1 Trend + TP/SL 1:2) เรียบร้อยครับ!")
    while True:
        try:
            check_trading_signal()
        except Exception as e:
            print("เกิดข้อผิดพลาดในการดึงข้อมูล:", e)
        time.sleep(60)

threading.Thread(target=bot_loop, daemon=True).start()
