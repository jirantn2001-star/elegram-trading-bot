import time
import requests
import yfinance as yf
import pandas as pd

TOKEN = "8977273894:AAEcJ-KSwZF7TZClxGaNauK76rGrzQ1I6D0"
CHAT_ID = "1484260985"

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    try:
        requests.post(url, json=payload)
    except Exception as e:
        print("Error sending message:", e)

send_telegram("🚀 บอท Cloud เริ่มทำงานแล้ว! กำลังเฝ้ากราฟให้อยู่ครับ")

def check_trading_signal():
    df = yf.download(tickers="GC=F", period="5d", interval="15m")
    if df.empty:
        return

    df['EMA9'] = df['Close'].ewm(span=9, adjust=False).mean()
    df['EMA21'] = df['Close'].ewm(span=21, adjust=False).mean()
    
    prev_ema9 = df['EMA9'].iloc[-2]
    prev_ema21 = df['EMA21'].iloc[-2]
    curr_ema9 = df['EMA9'].iloc[-1]
    curr_ema21 = df['EMA21'].iloc[-1]
    curr_price = df['Close'].iloc[-1]

    if prev_ema9 <= prev_ema21 and curr_ema9 > curr_ema21:
        msg = f"🟢 สัญญาณ BUY (ทองคำ XAUUSD)\nราคาปัจจุบัน: {curr_price:.2f}\nเงื่อนไข: EMA 9 ตัดขึ้นเหนือ EMA 21 เรียบร้อย!"
        send_telegram(msg)
        
    elif prev_ema9 >= prev_ema21 and curr_ema9 < curr_ema21:
        msg = f"🔴 สัญญาณ SELL (ทองคำ XAUUSD)\nราคาปัจจุบัน: {curr_price:.2f}\nเงื่อนไข: EMA 9 ตัดลงใต้ EMA 21 เรียบร้อย!"
        send_telegram(msg)

while True:
    try:
        check_trading_signal()
    except Exception as e:
        print("เกิดข้อผิดพลาดในการดึงข้อมูล:", e)
    time.sleep(60)
