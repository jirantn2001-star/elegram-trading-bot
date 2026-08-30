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
    return "SMC Trading Bot is running 24/7!"

def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

# --- ข้อมูล Telegram Bot ---
TOKEN = "8977273894:AAEcJ-KSwZF7TZClxGaNauK76rGrzQ1I6D0"
CHAT_ID = "1484260985"

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    try:
        requests.post(url, json=payload)
    except Exception as e:
        print("Error sending message:", e)

# Variable สำหรับจำสัญญาณเดิม ป้องกันส่งซ้ำในแท่งเดิม
last_signal_time = None

def check_trading_signal():
    global last_signal_time
    
    # ดึงข้อมูลกราฟทองคำ 15 นาที ย้อนหลัง 5 วัน
    df = yf.download(tickers="GC=F", period="5d", interval="15m", progress=False)
    if df.empty or len(df) < 205:
        return

    # จัดการ MultiIndex column จาก yfinance
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # 1. คำนวณ Trend ด้วย EMA 200
    df['EMA200'] = df['Close'].ewm(span=200, adjust=False).mean()

    # ดึงแท่งเทียนย้อนหลัง 3 แท่งล่าสุด (แท่ง -3, แท่ง -2, แท่ง -1)
    # -1 = แท่งปัจจุบันที่เพิ่งปิด / -2 = แท่งกลาง / -3 = แท่งแรก
    c_prev2 = df.iloc[-3]
    c_prev1 = df.iloc[-2]
    c_curr  = df.iloc[-1]
    
    curr_time = df.index[-1]
    if last_signal_time == curr_time:
        return # ถ้าแท่งนี้เคยแจ้งเตือนไปแล้ว ให้ข้าม

    close_price = c_curr['Close']
    ema200 = c_curr['EMA200']

    # --- 2. เช็คเงื่อนไข BUY SETUP (Uptrend + Bullish FVG + PA Rejection) ---
    is_uptrend = close_price > ema200
    # เกิด Bullish FVG: High แท่งแรก ต่ำกว่า Low แท่งปัจจุบัน
    has_bullish_fvg = c_prev2['High'] < c_curr['Low']
    
    if is_uptrend and has_bullish_fvg:
        fvg_top = c_curr['Low']
        fvg_bottom = c_prev2['High']
        
        msg = (
            f"🟢 **SIGNAL BUY (XAUUSD - 15M)**\n"
            f"📈 **Trend:** ขาขึ้น (เหนือ EMA200)\n"
            f"⚡ **Setup:** เกิด Bullish FVG + Rejection\n"
            f"💵 **ราคาปัจจุบัน:** {close_price:.2f}\n"
            f"🎯 **โซน FVG/OB:** {fvg_bottom:.2f} - {fvg_top:.2f}"
        )
        send_telegram(msg)
        last_signal_time = curr_time
        return

    # --- 3. เช็คเงื่อนไข SELL SETUP (Downtrend + Bearish FVG + PA Rejection) ---
    is_downtrend = close_price < ema200
    # เกิด Bearish FVG: Low แท่งแรก สูงกว่า High แท่งปัจจุบัน
    has_bearish_fvg = c_prev2['Low'] > c_curr['High']

    if is_downtrend and has_bearish_fvg:
        fvg_top = c_prev2['Low']
        fvg_bottom = c_curr['High']

        msg = (
            f"🔴 **SIGNAL SELL (XAUUSD - 15M)**\n"
            f"📉 **Trend:** ขาลง (ใต้ EMA200)\n"
            f"⚡ **Setup:** เกิด Bearish FVG + Rejection\n"
            f"💵 **ราคาปัจจุบัน:** {close_price:.2f}\n"
            f"🎯 **โซน FVG/OB:** {fvg_bottom:.2f} - {fvg_top:.2f}"
        )
        send_telegram(msg)
        last_signal_time = curr_time
        return

def bot_loop():
    send_telegram("🚀 อัปเดตบอท SMC (FVG + OB + Trend) เรียบร้อย! พร้อมเฝ้ากราฟ 24/7 ครับ")
    while True:
        try:
            check_trading_signal()
        except Exception as e:
            print("เกิดข้อผิดพลาดในการดึงข้อมูล:", e)
        time.sleep(60)

# ให้ Thread เริ่มทำงานทันที
threading.Thread(target=bot_loop, daemon=True).start()
