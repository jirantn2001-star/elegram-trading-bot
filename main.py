"""
XAUUSD (Gold) Multi-Timeframe SMC/PA Signal Bot
------------------------------------------------
- Data source : yfinance (GC=F)
- Web server  : Flask (port 8080) สำหรับให้ Render มองเห็นว่า service ยัง alive
- Bot loop    : รันแยกด้วย threading เพื่อสแกนสัญญาณต่อเนื่อง 24/7
- Strategy    : Trend filter (EMA200 บน H1) + Fair Value Gap (FVG) บน 5M
- Money Mgmt  : RR 1:2, SL = ปลาย FVG +/- 1.0 USD
- Notify      : Telegram Bot API (Markdown)

Environment Variables ที่ต้องตั้งค่าบน Render:
    TELEGRAM_BOT_TOKEN   -> Token ของบอทจาก BotFather
    TELEGRAM_CHAT_ID     -> Chat ID ปลายทางที่จะรับการแจ้งเตือน
    SYMBOL               -> (optional) default = "GC=F"
    SCAN_INTERVAL_SEC    -> (optional) default = 60 วินาที
"""

import os
import time
import threading
import logging
from datetime import datetime, timezone

import requests
import pandas as pd
import yfinance as yf
from flask import Flask, jsonify

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SYMBOL = os.environ.get("SYMBOL", "GC=F")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
SCAN_INTERVAL_SEC = int(os.environ.get("SCAN_INTERVAL_SEC", "60"))

EMA_PERIOD = 200
RISK_REWARD_RATIO = 2.0
SL_BUFFER_USD = 1.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("xauusd-bot")

# เก็บ timestamp ของแท่งเทียนล่าสุดที่เคยยิงสัญญาณไปแล้ว เพื่อกันยิงซ้ำ
last_signal_state = {
    "candle_time": None,
    "direction": None,
}

# ---------------------------------------------------------------------------
# FLASK APP (สำหรับ Render Web Service / Health Check)
# ---------------------------------------------------------------------------
app = Flask(__name__)


@app.route("/")
def home():
    return jsonify(
        {
            "status": "running",
            "symbol": SYMBOL,
            "last_signal_candle": str(last_signal_state["candle_time"]),
            "last_signal_direction": last_signal_state["direction"],
            "server_time_utc": datetime.now(timezone.utc).isoformat(),
        }
    )


@app.route("/health")
def health():
    return "OK", 200


# ---------------------------------------------------------------------------
# TELEGRAM
# ---------------------------------------------------------------------------
def send_telegram_message(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("ยังไม่ได้ตั้งค่า TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID จึงข้ามการส่งข้อความ")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(url, data=payload, timeout=15)
        if resp.status_code != 200:
            log.error("ส่งข้อความ Telegram ไม่สำเร็จ: %s", resp.text)
    except Exception as e:  # noqa: BLE001
        log.error("เกิดข้อผิดพลาดตอนส่ง Telegram: %s", e)


def format_signal_message(
    direction: str,
    entry: float,
    sl: float,
    tp: float,
    fvg_top: float,
    fvg_bottom: float,
    candle_time,
) -> str:
    emoji = "🟢" if direction == "BUY" else "🔴"
    risk = abs(entry - sl)
    reward = abs(tp - entry)

    msg = (
        f"{emoji} *SIGNAL {direction}* — `{SYMBOL}` (XAUUSD)\n"
        f"────────────────────\n"
        f"🕒 *Timeframe:* 5M (Trend filter H1)\n"
        f"📌 *Entry:* `{entry:.2f}`\n"
        f"🛑 *Stop Loss:* `{sl:.2f}`  (Risk ≈ {risk:.2f} USD)\n"
        f"🎯 *Take Profit:* `{tp:.2f}`  (Reward ≈ {reward:.2f} USD)\n"
        f"⚖️ *Risk:Reward:* 1:{RISK_REWARD_RATIO:.1f}\n"
        f"📐 *FVG Zone:* `{fvg_bottom:.2f}` - `{fvg_top:.2f}`\n"
        f"🕯️ *Candle Time (UTC):* `{candle_time}`\n"
        f"────────────────────\n"
        f"_สัญญาณจากระบบ SMC + Price Action (EMA200 H1 + FVG 5M)_"
    )
    return msg


# ---------------------------------------------------------------------------
# DATA FETCHING
# ---------------------------------------------------------------------------
def fetch_h1_data() -> pd.DataFrame:
    """ดึงข้อมูล H1 ย้อนหลังพอสำหรับคำนวณ EMA200"""
    df = yf.download(
        tickers=SYMBOL,
        period="60d",
        interval="1h",
        progress=False,
        auto_adjust=False,
    )
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.dropna()
    return df


def fetch_5m_data() -> pd.DataFrame:
    """ดึงข้อมูล 5M สำหรับสแกน FVG (yfinance จำกัดข้อมูล intraday ~60 วันย้อนหลัง)"""
    df = yf.download(
        tickers=SYMBOL,
        period="5d",
        interval="5m",
        progress=False,
        auto_adjust=False,
    )
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.dropna()
    return df


# ---------------------------------------------------------------------------
# STRATEGY LOGIC
# ---------------------------------------------------------------------------
def get_h1_trend(df_h1: pd.DataFrame) -> str:
    """คืนค่า 'UP' / 'DOWN' / 'NONE' จากราคาปิดล่าสุดเทียบ EMA200 บน H1"""
    if len(df_h1) < EMA_PERIOD:
        log.warning("ข้อมูล H1 ไม่พอสำหรับคำนวณ EMA%d (มี %d แท่ง)", EMA_PERIOD, len(df_h1))
        return "NONE"

    df_h1 = df_h1.copy()
    df_h1["EMA200"] = df_h1["Close"].ewm(span=EMA_PERIOD, adjust=False).mean()

    last_close = float(df_h1["Close"].iloc[-1])
    last_ema = float(df_h1["EMA200"].iloc[-1])

    if last_close > last_ema:
        return "UP"
    elif last_close < last_ema:
        return "DOWN"
    return "NONE"


def detect_fvg(df_5m: pd.DataFrame, trend: str):
    """
    ตรวจสอบ Fair Value Gap บนแท่งล่าสุด 3 แท่ง (prev2, prev1, current)

    Bullish FVG: High(prev2) < Low(current)  -> เกิดในเทรนขาขึ้น
    Bearish FVG: Low(prev2)  > High(current) -> เกิดในเทรนขาลง

    คืนค่า dict สัญญาณ หรือ None ถ้าไม่มีสัญญาณ
    """
    if len(df_5m) < 3:
        return None

    prev2 = df_5m.iloc[-3]
    current = df_5m.iloc[-1]
    candle_time = df_5m.index[-1]

    high_prev2 = float(prev2["High"])
    low_prev2 = float(prev2["Low"])
    high_curr = float(current["High"])
    low_curr = float(current["Low"])
    close_curr = float(current["Close"])

    # ----- BUY: เทรนขาขึ้น + Bullish FVG -----
    if trend == "UP" and high_prev2 < low_curr:
        fvg_bottom = high_prev2   # ปลาย FVG ด้านล่าง
        fvg_top = low_curr        # ปลาย FVG ด้านบน

        entry = close_curr
        sl = fvg_bottom - SL_BUFFER_USD
        risk = entry - sl
        if risk <= 0:
            return None
        tp = entry + (risk * RISK_REWARD_RATIO)

        return {
            "direction": "BUY",
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "fvg_top": fvg_top,
            "fvg_bottom": fvg_bottom,
            "candle_time": candle_time,
        }

    # ----- SELL: เทรนขาลง + Bearish FVG -----
    if trend == "DOWN" and low_prev2 > high_curr:
        fvg_top = low_prev2       # ปลาย FVG ด้านบน
        fvg_bottom = high_curr    # ปลาย FVG ด้านล่าง

        entry = close_curr
        sl = fvg_top + SL_BUFFER_USD
        risk = sl - entry
        if risk <= 0:
            return None
        tp = entry - (risk * RISK_REWARD_RATIO)

        return {
            "direction": "SELL",
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "fvg_top": fvg_top,
            "fvg_bottom": fvg_bottom,
            "candle_time": candle_time,
        }

    return None


# ---------------------------------------------------------------------------
# MAIN SCAN LOOP
# ---------------------------------------------------------------------------
def scan_once():
    try:
        df_h1 = fetch_h1_data()
        trend = get_h1_trend(df_h1)
        log.info("H1 Trend: %s", trend)

        if trend == "NONE":
            return

        df_5m = fetch_5m_data()
        signal = detect_fvg(df_5m, trend)

        if signal is None:
            log.info("ไม่พบสัญญาณในรอบนี้ (Trend=%s)", trend)
            return

        candle_time = signal["candle_time"]

        # ----- กันยิงสัญญาณซ้ำในแท่งเทียนเดิม -----
        if (
            last_signal_state["candle_time"] == candle_time
            and last_signal_state["direction"] == signal["direction"]
        ):
            log.info("สัญญาณ %s ที่แท่ง %s เคยยิงไปแล้ว ข้ามรอบนี้", signal["direction"], candle_time)
            return

        msg = format_signal_message(
            direction=signal["direction"],
            entry=signal["entry"],
            sl=signal["sl"],
            tp=signal["tp"],
            fvg_top=signal["fvg_top"],
            fvg_bottom=signal["fvg_bottom"],
            candle_time=candle_time,
        )
        send_telegram_message(msg)
        log.info("ส่งสัญญาณ %s สำเร็จ (Entry=%.2f SL=%.2f TP=%.2f)",
                  signal["direction"], signal["entry"], signal["sl"], signal["tp"])

        last_signal_state["candle_time"] = candle_time
        last_signal_state["direction"] = signal["direction"]

    except Exception as e:  # noqa: BLE001
        log.exception("เกิดข้อผิดพลาดระหว่างสแกน: %s", e)


def bot_loop():
    log.info("เริ่มการทำงานของ Bot Loop (scan ทุก %d วินาที)", SCAN_INTERVAL_SEC)
    while True:
        scan_once()
        time.sleep(SCAN_INTERVAL_SEC)


# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------
def start_background_thread():
    t = threading.Thread(target=bot_loop, daemon=True)
    t.start()


start_background_thread()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
