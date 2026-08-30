"""
XAUUSD (Gold) Multi-Timeframe SMC/PA Signal Bot — v2
------------------------------------------------------
- Data source : yfinance (GC=F)
- Web server  : Flask (port 8080) สำหรับให้ Render มองเห็นว่า service ยัง alive
- Bot loop    : รันแยกด้วย threading เพื่อสแกนสัญญาณต่อเนื่อง 24/7
- Strategy    : Trend filter (EMA200 บน H1) + Fair Value Gap (FVG) บน 5M
- Money Mgmt  : RR 1:2, SL = ปลาย FVG +/- 1.0 USD
- Notify      : Telegram Bot API (Markdown)

--- สิ่งที่เพิ่มใน v2 ---
1. Active Signal Storage : เก็บสัญญาณที่ยังไม่ปิดไว้ใน active_signals (list of dict)
2. Price Monitoring      : ทุกรอบ scan จะเช็ค High/Low ของแท่ง 5M ล่าสุด เทียบกับ TP/SL
                           ของทุกสัญญาณที่ยังค้างอยู่ ว่าโดนชนหรือยัง
3. Follow-up Alert       : ยิง Telegram แยกเมื่อ TP หรือ SL โดน แล้วลบสัญญาณนั้นออกจากรายการ
4. Weekly Summary        : สรุปผล Win/Loss ทุกวันศุกร์ 23:50 UTC แล้ว reset ตัวนับ
5. Memory Management     : ลบสัญญาณที่ค้างเกิน 24 ชม. ทิ้งอัตโนมัติ (ไม่นับเป็น Win/Loss)

Environment Variables ที่ต้องตั้งค่าบน Render:
    TELEGRAM_BOT_TOKEN   -> Token ของบอทจาก BotFather
    TELEGRAM_CHAT_ID     -> Chat ID ปลายทางที่จะรับการแจ้งเตือน
    SYMBOL               -> (optional) default = "GC=F"
    SCAN_INTERVAL_SEC    -> (optional) default = 60 วินาที
    SIGNAL_TIMEOUT_HOURS -> (optional) default = 24 ชั่วโมง
"""

import os
import time
import threading
import logging
import itertools
from datetime import datetime, timezone, timedelta

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
SIGNAL_TIMEOUT_HOURS = float(os.environ.get("SIGNAL_TIMEOUT_HOURS", "24"))

EMA_PERIOD = 200
RISK_REWARD_RATIO = 2.0
SL_BUFFER_USD = 1.0

# วันศุกร์ = weekday() == 4 (จันทร์=0 ... อาทิตย์=6), เวลา UTC
WEEKLY_SUMMARY_WEEKDAY = 4
WEEKLY_SUMMARY_HOUR_UTC = 23
WEEKLY_SUMMARY_MINUTE_UTC = 50

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("xauusd-bot")

# เก็บ timestamp ของแท่งเทียนล่าสุดที่เคยยิงสัญญาณ "เปิด" ไปแล้ว เพื่อกันยิงซ้ำ
last_signal_state = {
    "candle_time": None,
    "direction": None,
}

# ---------------------------------------------------------------------------
# STATE สำหรับระบบ Journal (v2)
# ---------------------------------------------------------------------------
# รายการสัญญาณที่ "เปิด" อยู่ ยังไม่ชน TP/SL
# แต่ละ item: {id, direction, entry, sl, tp, candle_time, opened_at}
active_signals: list[dict] = []

# ตัวสร้าง id ให้สัญญาณแต่ละอันไม่ซ้ำกัน (ใช้ในการอ้างอิง/debug เท่านั้น)
_signal_id_counter = itertools.count(1)

# ตัวนับผลประจำสัปดาห์ สำหรับสรุปทุกวันศุกร์
weekly_stats = {
    "signals": 0,
    "wins": 0,
    "losses": 0,
    "timeouts": 0,  # สัญญาณที่ถูกล้างทิ้งเพราะค้างนานเกินไป (ไม่นับ win/loss)
}

# เก็บ (ปี, เลขสัปดาห์ ISO) ของรอบล่าสุดที่เคยส่งสรุปไปแล้ว กันส่งซ้ำในหน้าต่าง 10 นาทีของวันศุกร์
_last_weekly_summary_sent_isoweek = None

# lock ป้องกัน race condition ระหว่าง thread หลัก (bot_loop) กับ endpoint Flask ที่มาอ่านค่า
state_lock = threading.Lock()


# ---------------------------------------------------------------------------
# FLASK APP (สำหรับ Render Web Service / Health Check)
# ---------------------------------------------------------------------------
app = Flask(__name__)


@app.route("/")
def home():
    with state_lock:
        return jsonify(
            {
                "status": "running",
                "symbol": SYMBOL,
                "last_signal_candle": str(last_signal_state["candle_time"]),
                "last_signal_direction": last_signal_state["direction"],
                "active_signals_count": len(active_signals),
                "active_signals": active_signals,
                "weekly_stats": weekly_stats,
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


def format_result_message(signal: dict, result: str, hit_price: float) -> str:
    """
    result: 'TP' หรือ 'SL'
    สร้างข้อความแจ้งผลลัพธ์แบบเห็นชัดเจน (Win/Loss)
    """
    if result == "TP":
        emoji = "🎯"
        title = f"WIN: TP Hit (+{RISK_REWARD_RATIO:.0f}R)"
    else:
        emoji = "🛑"
        title = "LOSS: SL Hit (-1R)"

    opened_at = signal["opened_at"]
    duration = datetime.now(timezone.utc) - opened_at
    duration_min = int(duration.total_seconds() // 60)

    msg = (
        f"{emoji} *{title}*\n"
        f"────────────────────\n"
        f"📌 *{signal['direction']}* `{SYMBOL}` @ `{signal['entry']:.2f}`\n"
        f"💰 *ราคาที่ชน:* `{hit_price:.2f}`\n"
        f"🎯 TP: `{signal['tp']:.2f}`   🛑 SL: `{signal['sl']:.2f}`\n"
        f"⏱️ *ถือสัญญาณ:* {duration_min} นาที\n"
        f"🕯️ *เปิดสัญญาณที่แท่ง:* `{signal['candle_time']}`\n"
        f"────────────────────"
    )
    return msg


def format_weekly_summary_message() -> str:
    signals = weekly_stats["signals"]
    wins = weekly_stats["wins"]
    losses = weekly_stats["losses"]
    timeouts = weekly_stats["timeouts"]
    resolved = wins + losses
    win_rate = (wins / resolved * 100) if resolved > 0 else 0.0

    msg = (
        f"📊 *Weekly Performance Summary* — `{SYMBOL}`\n"
        f"────────────────────\n"
        f"📨 *สัญญาณทั้งหมด:* {signals}\n"
        f"✅ *Win:* {wins}\n"
        f"❌ *Loss:* {losses}\n"
        f"⌛ *Timeout (ค้างเกิน {SIGNAL_TIMEOUT_HOURS:.0f} ชม.):* {timeouts}\n"
        f"🏆 *Win Rate:* {win_rate:.0f}%\n"
        f"────────────────────\n"
        f"_สรุปสัปดาห์นี้ — ตัวนับจะถูกรีเซ็ตสำหรับสัปดาห์ถัดไป_"
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
# (ใหม่ v2) SIGNAL MONITORING — เช็คว่าสัญญาณที่เปิดค้างไว้ชน TP/SL หรือยัง
# ---------------------------------------------------------------------------
def monitor_active_signals(df_5m: pd.DataFrame) -> None:
    """
    ใช้ High/Low ของแท่ง 5M ล่าสุดมาเช็คกับทุกสัญญาณใน active_signals
    - BUY : Low <= SL  -> โดน SL (Loss) | High >= TP -> โดน TP (Win)
    - SELL: High >= SL -> โดน SL (Loss) | Low <= TP  -> โดน TP (Win)

    ถ้าราคาแกว่งชนทั้ง TP และ SL ในแท่งเดียวกัน (กรณี volatility สูง)
    จะยึด SL เป็นหลักก่อน เพื่อความระมัดระวังสูงสุด (conservative)
    """
    if df_5m.empty or not active_signals:
        return

    latest_candle = df_5m.iloc[-1]
    high = float(latest_candle["High"])
    low = float(latest_candle["Low"])

    still_active = []

    with state_lock:
        for signal in active_signals:
            direction = signal["direction"]
            sl = signal["sl"]
            tp = signal["tp"]

            hit_result = None
            hit_price = None

            if direction == "BUY":
                if low <= sl:
                    hit_result = "SL"
                    hit_price = sl
                elif high >= tp:
                    hit_result = "TP"
                    hit_price = tp
            elif direction == "SELL":
                if high >= sl:
                    hit_result = "SL"
                    hit_price = sl
                elif low <= tp:
                    hit_result = "TP"
                    hit_price = tp

            if hit_result is not None:
                # ----- ยิงข้อความแจ้งผล แล้วอัปเดตสถิติสัปดาห์ -----
                msg = format_result_message(signal, hit_result, hit_price)
                send_telegram_message(msg)

                if hit_result == "TP":
                    weekly_stats["wins"] += 1
                    log.info("สัญญาณ id=%s (%s) ชน TP", signal["id"], direction)
                else:
                    weekly_stats["losses"] += 1
                    log.info("สัญญาณ id=%s (%s) ชน SL", signal["id"], direction)
                # ไม่เก็บสัญญาณนี้ต่อ (ลบออกจากลิสต์)
            else:
                # ----- (ใหม่ v2) Memory Management: ล้างสัญญาณที่ค้างนานเกินไป -----
                age = datetime.now(timezone.utc) - signal["opened_at"]
                if age > timedelta(hours=SIGNAL_TIMEOUT_HOURS):
                    weekly_stats["timeouts"] += 1
                    log.info(
                        "สัญญาณ id=%s (%s) ค้างเกิน %.0f ชม. ลบทิ้งเพื่อกัน memory leak",
                        signal["id"], direction, SIGNAL_TIMEOUT_HOURS,
                    )
                    send_telegram_message(
                        f"⌛ *TIMEOUT:* สัญญาณ {direction} `{SYMBOL}` @ `{signal['entry']:.2f}` "
                        f"ค้างเกิน {SIGNAL_TIMEOUT_HOURS:.0f} ชม. ยังไม่ชน TP/SL จึงลบออกจากระบบติดตาม"
                    )
                    # ไม่นับ win/loss เพราะยังไม่รู้ผลจริง
                else:
                    still_active.append(signal)

        active_signals[:] = still_active


# ---------------------------------------------------------------------------
# (ใหม่ v2) WEEKLY SUMMARY — ส่งสรุปทุกวันศุกร์ 23:50 UTC
# ---------------------------------------------------------------------------
def check_and_send_weekly_summary() -> None:
    global _last_weekly_summary_sent_isoweek

    now = datetime.now(timezone.utc)
    is_friday = now.weekday() == WEEKLY_SUMMARY_WEEKDAY
    is_summary_time = (
        now.hour == WEEKLY_SUMMARY_HOUR_UTC and now.minute >= WEEKLY_SUMMARY_MINUTE_UTC
    )

    if not (is_friday and is_summary_time):
        return

    iso_year, iso_week, _ = now.isocalendar()
    current_key = (iso_year, iso_week)

    # กันส่งซ้ำหลายครั้งในหน้าต่าง 10 นาทีสุดท้ายของวันศุกร์ (loop รันทุก SCAN_INTERVAL_SEC)
    if _last_weekly_summary_sent_isoweek == current_key:
        return

    msg = format_weekly_summary_message()
    send_telegram_message(msg)
    log.info("ส่งสรุปผลประจำสัปดาห์แล้ว: %s", weekly_stats)

    # ----- Reset ตัวนับสำหรับสัปดาห์ถัดไป -----
    with state_lock:
        weekly_stats["signals"] = 0
        weekly_stats["wins"] = 0
        weekly_stats["losses"] = 0
        weekly_stats["timeouts"] = 0

    _last_weekly_summary_sent_isoweek = current_key


# ---------------------------------------------------------------------------
# MAIN SCAN LOOP
# ---------------------------------------------------------------------------
def scan_once():
    try:
        df_h1 = fetch_h1_data()
        trend = get_h1_trend(df_h1)
        log.info("H1 Trend: %s", trend)

        df_5m = fetch_5m_data()

        # (v2) ทุกรอบต้องเช็คสัญญาณที่ค้างอยู่ก่อน ไม่ว่าจะมีเทรนใหม่หรือไม่
        monitor_active_signals(df_5m)

        # (v2) เช็คว่าถึงเวลาส่งสรุปประจำสัปดาห์หรือยัง (ทำทุกรอบ ไม่ผูกกับเทรน)
        check_and_send_weekly_summary()

        if trend == "NONE":
            return

        signal = detect_fvg(df_5m, trend)

        if signal is None:
            log.info("ไม่พบสัญญาณใหม่ในรอบนี้ (Trend=%s)", trend)
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

        # ----- (v2) บันทึกสัญญาณนี้ลง active_signals เพื่อติดตามผลต่อ -----
        with state_lock:
            active_signals.append(
                {
                    "id": next(_signal_id_counter),
                    "direction": signal["direction"],
                    "entry": signal["entry"],
                    "sl": signal["sl"],
                    "tp": signal["tp"],
                    "candle_time": str(candle_time),
                    "opened_at": datetime.now(timezone.utc),
                }
            )
            weekly_stats["signals"] += 1

    except Exception as e:  # noqa: BLE001
        log.exception("เกิดข้อผิดพลาดระหว่างสแกน: %s", e)


# --- HEALTH CHECK ROUTE ---
@app.route("/")
def health_check():
    return {"status": "ok", "bot": "running"}, 200

# --- BOT LOOP FUNCTION ---
def bot_loop():
    log.info("เริ่มการทำงานของ Bot Loop (scan ทุก %d วินาที)", SCAN_INTERVAL_SEC)
    while True:
        try:
            scan_once()
        except Exception as e:
            log.exception("เกิดข้อผิดพลาดใน bot_loop: %s", e)
        time.sleep(SCAN_INTERVAL_SEC)

# --- START THREAD WITH APP CONTEXT ---
with app.app_context():
    bot_thread = threading.Thread(target=bot_loop, daemon=True)
    bot_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
