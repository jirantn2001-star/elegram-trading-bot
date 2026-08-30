"""
XAUUSD (Gold) Multi-Timeframe SMC/PA Signal Bot — v4
------------------------------------------------------
- Data source : yfinance (GC=F)
- Web server  : Flask (port 8080) สำหรับให้ Render มองเห็นว่า service ยัง alive
- Bot loop    : รันแยกด้วย threading เพื่อสแกนสัญญาณต่อเนื่อง 24/7

--- Strategy: ต้องผ่านครบ 4 เงื่อนไขถึงจะยิงสัญญาณ ---
1. Trend Filter (H1)         : ราคาต้องอยู่เหนือ/ใต้ EMA200 บน Timeframe 1 ชั่วโมง
2. Support/Resistance (15M)  : ราคาปัจจุบันต้องอยู่ใกล้แนวรับ (BUY) หรือแนวต้าน (SELL)
                                ที่หาได้จาก Swing High/Low (fractal) บน Timeframe 15 นาที
                                -> ถ้าไม่มีแนวรับ/แนวต้านอยู่ใกล้ราคาปัจจุบัน จะไม่ยิงสัญญาณ
3. Price Action (5M)         : ต้องเกิดแท่งเทียนยืนยันการกลับตัว (Rejection/Pin Bar) ที่แนวนั้น
                                -> BUY: แท่งเขียว มีไส้ล่างยาว (แรงซื้อดันราคากลับจากแนวรับ)
                                -> SELL: แท่งแดง มีไส้บนยาว (แรงขายดันราคากลับจากแนวต้าน)
                                -> (ใหม่ v4) Volume ของแท่งนั้นต้องสูงกว่าค่าเฉลี่ย Volume 20 แท่งย้อนหลัง
4. Time Session (ใหม่ v4)    : เปิดสัญญาณใหม่เฉพาะช่วง London/New York Session
                                UTC 07:00 - 16:00 (ตรงกับเวลาไทย 14:00 - 23:00 น.)
                                -> นอกช่วงนี้บอทยังคง monitor สัญญาณเดิม/ส่งสรุปได้ปกติ
                                   แค่ไม่เปิดสัญญาณใหม่

--- Money Mgmt: Dynamic SL ด้วย ATR (ใหม่ v4, RR 1:2) ---
- คำนวณ ATR (Period 14) บน TF 5M
- SL ฝั่ง Buy  = Low ของแท่งยืนยัน  - (1.5 x ATR)
- SL ฝั่ง Sell = High ของแท่งยืนยัน + (1.5 x ATR)
- TP           = ระยะ Risk (Entry-SL) x 2 (RR 1:2 เหมือนเดิม)

--- Journal Features ---
1. Active Signal Storage : เก็บสัญญาณที่ยังไม่ปิดไว้ใน active_signals (list of dict)
2. Price Monitoring      : ทุกรอบ scan จะเช็ค High/Low ของแท่ง 5M ล่าสุด เทียบกับ TP/SL
3. Follow-up Alert       : ยิง Telegram แยกเมื่อ TP หรือ SL โดน แล้วลบสัญญาณนั้นออกจากรายการ
4. Weekly Summary        : สรุปผล Win/Loss ทุกวันศุกร์ 23:50 UTC แล้ว reset ตัวนับ
5. Memory Management     : ลบสัญญาณที่ค้างเกิน 24 ชม. ทิ้งอัตโนมัติ (ไม่นับเป็น Win/Loss)

--- Data Stability (ใหม่ v4) ---
- ห่อ yf.download() ด้วย try/except กันโปรแกรม Crash เวลาเน็ตหลุด/API ล่ม
- ทำความสะอาดข้อมูล: แทนที่ ±inf ด้วย NaN, forward-fill ช่องว่างสั้นๆ, แล้วค่อยตัดแถวที่ยังว่างอยู่
- ทุกจุดที่ดึงข้อมูลจะเช็ค DataFrame ว่างเปล่าก่อนใช้งานเสมอ

Environment Variables ที่ต้องตั้งค่าบน Render:
    TELEGRAM_BOT_TOKEN   -> Token ของบอทจาก BotFather
    TELEGRAM_CHAT_ID     -> Chat ID ปลายทางที่จะรับการแจ้งเตือน
    SYMBOL               -> (optional) default = "GC=F"
    SCAN_INTERVAL_SEC    -> (optional) default = 60 วินาที
    SIGNAL_TIMEOUT_HOURS -> (optional) default = 24 ชั่วโมง
    SR_TOLERANCE_USD     -> (optional) default = 3.0  ระยะห่างที่ยอมรับว่า "ราคาอยู่ใกล้แนวรับ/ต้าน"
    SR_FRACTAL_WINDOW    -> (optional) default = 2    จำนวนแท่งซ้าย/ขวาที่ใช้หา Swing High/Low
    ATR_PERIOD           -> (optional) default = 14   จำนวนแท่งที่ใช้คำนวณ ATR บน 5M
    ATR_SL_MULTIPLIER    -> (optional) default = 1.5  ตัวคูณ ATR สำหรับกำหนดระยะ SL
    VOLUME_MA_PERIOD     -> (optional) default = 20   จำนวนแท่งย้อนหลังสำหรับ Volume MA
    SESSION_START_HOUR_UTC -> (optional) default = 7  เริ่มช่วงเทรด (UTC)
    SESSION_END_HOUR_UTC   -> (optional) default = 16 สิ้นสุดช่วงเทรด (UTC)
"""

import os
import time
import threading
import logging
import itertools
from datetime import datetime, timezone, timedelta

import requests
import numpy as np
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

# ----- (ใหม่ v4) Dynamic SL ด้วย ATR แทนค่าคงที่ 1.0 USD -----
ATR_PERIOD = int(os.environ.get("ATR_PERIOD", "14"))
ATR_SL_MULTIPLIER = float(os.environ.get("ATR_SL_MULTIPLIER", "1.5"))

# ----- (ใหม่ v4) Volume Filter บน 5M -----
VOLUME_MA_PERIOD = int(os.environ.get("VOLUME_MA_PERIOD", "20"))

# ----- (ใหม่ v4) Time Session Filter (London/New York, UTC) -----
SESSION_START_HOUR_UTC = int(os.environ.get("SESSION_START_HOUR_UTC", "7"))
SESSION_END_HOUR_UTC = int(os.environ.get("SESSION_END_HOUR_UTC", "16"))

# ----- PA แนวรับ-แนวต้าน บน TF 15M -----
SR_TOLERANCE_USD = float(os.environ.get("SR_TOLERANCE_USD", "3.0"))
SR_FRACTAL_WINDOW = int(os.environ.get("SR_FRACTAL_WINDOW", "2"))

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
# STATE สำหรับระบบ Journal
# ---------------------------------------------------------------------------
active_signals: list[dict] = []
_signal_id_counter = itertools.count(1)

weekly_stats = {
    "signals": 0,
    "wins": 0,
    "losses": 0,
    "timeouts": 0,
}

_last_weekly_summary_sent_isoweek = None
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
    pattern: str,
    candle_time,
    pa_level: float,
    atr_value: float,
) -> str:
    emoji = "🟢" if direction == "BUY" else "🔴"
    risk = abs(entry - sl)
    reward = abs(tp - entry)
    pa_label = "แนวรับ (Support)" if direction == "BUY" else "แนวต้าน (Resistance)"

    msg = (
        f"{emoji} *SIGNAL {direction}* — `{SYMBOL}` (XAUUSD)\n"
        f"────────────────────\n"
        f"🕒 *Timeframe:* 5M (Trend H1 + S/R 15M)\n"
        f"📌 *Entry:* `{entry:.2f}`\n"
        f"🛑 *Stop Loss:* `{sl:.2f}`  (Risk ≈ {risk:.2f} USD)\n"
        f"🎯 *Take Profit:* `{tp:.2f}`  (Reward ≈ {reward:.2f} USD)\n"
        f"⚖️ *Risk:Reward:* 1:{RISK_REWARD_RATIO:.1f}\n"
        f"📏 *ATR(14) 5M:* `{atr_value:.2f}` (SL = {ATR_SL_MULTIPLIER:.1f} x ATR)\n"
        f"🧱 *{pa_label} (15M):* `{pa_level:.2f}` (ห่างจาก Entry {abs(entry - pa_level):.2f} USD)\n"
        f"🕯️ *Price Action:* {pattern} (Volume ยืนยันแล้ว)\n"
        f"🕐 *Candle Time (UTC):* `{candle_time}`\n"
        f"────────────────────\n"
        f"_ผ่านครบ 4 เงื่อนไข: Trend(H1) + S/R(15M) + Price Action+Volume(5M) + Session_"
    )
    return msg


def format_result_message(signal: dict, result: str, hit_price: float) -> str:
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
def _clean_ohlcv_dataframe(df: pd.DataFrame, label: str) -> pd.DataFrame:
    """
    (ใหม่ v4) ทำความสะอาดข้อมูล OHLCV จาก yfinance เพื่อกัน Crash:
    - ถ้า df เป็น None/ว่างเปล่า -> คืน DataFrame ว่างแทนการปล่อยให้โค้ดถัดไป error
    - แปลง MultiIndex columns (บางครั้ง yfinance คืนมาแบบนี้) ให้แบนราบ
    - แทนที่ ±inf ด้วย NaN, forward-fill ช่องว่างสั้นๆ ที่ขาดหาย แล้วค่อยตัดแถวที่ยังว่างอยู่จริงๆ
    """
    if df is None or df.empty:
        log.warning("ไม่ได้รับข้อมูลจาก yfinance สำหรับ %s (ผลลัพธ์ว่างเปล่า/None)", label)
        return pd.DataFrame()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.ffill()  # เติมค่าที่ขาดหายด้วยค่าแท่งก่อนหน้า กันข้อมูลกระโดดหายเป็นหย่อมๆ
    df = df.dropna()  # ตัดแถวที่ยังว่างอยู่จริง (เช่น แถวแรกสุดที่ไม่มีค่าก่อนหน้าให้ ffill)

    if df.empty:
        log.warning("ข้อมูล %s หลังทำความสะอาดแล้วว่างเปล่า (อาจเป็นช่วงตลาดปิด)", label)

    return df


def fetch_h1_data() -> pd.DataFrame:
    """ดึงข้อมูล H1 ย้อนหลังพอสำหรับคำนวณ EMA200"""
    try:
        df = yf.download(
            tickers=SYMBOL,
            period="60d",
            interval="1h",
            progress=False,
            auto_adjust=False,
        )
    except Exception as e:  # noqa: BLE001
        log.error("ดึงข้อมูล H1 จาก yfinance ล้มเหลว: %s", e)
        return pd.DataFrame()
    return _clean_ohlcv_dataframe(df, "H1")


def fetch_5m_data() -> pd.DataFrame:
    """ดึงข้อมูล 5M สำหรับตรวจแท่งยืนยัน Price Action (yfinance จำกัดข้อมูล intraday ~60 วันย้อนหลัง)"""
    try:
        df = yf.download(
            tickers=SYMBOL,
            period="5d",
            interval="5m",
            progress=False,
            auto_adjust=False,
        )
    except Exception as e:  # noqa: BLE001
        log.error("ดึงข้อมูล 5M จาก yfinance ล้มเหลว: %s", e)
        return pd.DataFrame()
    return _clean_ohlcv_dataframe(df, "5M")


def fetch_15m_data() -> pd.DataFrame:
    """ดึงข้อมูล 15M สำหรับหาแนวรับ-แนวต้านด้วย Price Action"""
    try:
        df = yf.download(
            tickers=SYMBOL,
            period="30d",
            interval="15m",
            progress=False,
            auto_adjust=False,
        )
    except Exception as e:  # noqa: BLE001
        log.error("ดึงข้อมูล 15M จาก yfinance ล้มเหลว: %s", e)
        return pd.DataFrame()
    return _clean_ohlcv_dataframe(df, "15M")


# ---------------------------------------------------------------------------
# STRATEGY LOGIC
# ---------------------------------------------------------------------------
def get_h1_trend(df_h1: pd.DataFrame) -> str:
    """เงื่อนไขที่ 1: คืนค่า 'UP' / 'DOWN' / 'NONE' จากราคาปิดล่าสุดเทียบ EMA200 บน H1"""
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


def calculate_atr(df_5m: pd.DataFrame, period: int = ATR_PERIOD):
    """
    (ใหม่ v4) คำนวณ ATR (Average True Range) แบบ Wilder's smoothing บน TF 5M
    True Range = max(High-Low, |High-PrevClose|, |Low-PrevClose|)
    ATR         = ค่าเฉลี่ยเคลื่อนที่แบบ exponential ของ True Range (alpha = 1/period)

    คืนค่า ATR ล่าสุด (float) หรือ None ถ้าข้อมูลไม่พอ
    """
    if df_5m.empty or len(df_5m) < period + 1:
        log.warning("ข้อมูล 5M ไม่พอสำหรับคำนวณ ATR(%d) (มี %d แท่ง)", period, len(df_5m))
        return None

    high = df_5m["High"]
    low = df_5m["Low"]
    prev_close = df_5m["Close"].shift(1)

    true_range = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr_series = true_range.ewm(alpha=1 / period, adjust=False).mean()
    atr_value = atr_series.iloc[-1]

    if pd.isna(atr_value):
        return None
    return float(atr_value)


def check_volume_confirmation(df_5m: pd.DataFrame, ma_period: int = VOLUME_MA_PERIOD) -> bool:
    """
    (ใหม่ v4) เช็คว่า Volume ของแท่งล่าสุดสูงกว่าค่าเฉลี่ย Volume ของ `ma_period`
    แท่งก่อนหน้าหรือไม่ (ไม่รวมแท่งปัจจุบันในค่าเฉลี่ย เพื่อกัน bias)

    คืนค่า True ถ้า Volume ยืนยัน (สูงกว่าค่าเฉลี่ย), False ถ้าไม่ผ่านหรือข้อมูลไม่พอ
    """
    if "Volume" not in df_5m.columns or len(df_5m) < ma_period + 1:
        log.warning("ข้อมูล Volume ไม่พอสำหรับคำนวณ Volume MA(%d)", ma_period)
        return False

    current_volume = float(df_5m["Volume"].iloc[-1])
    prior_volumes = df_5m["Volume"].iloc[-(ma_period + 1) : -1]
    volume_ma = float(prior_volumes.mean())

    if pd.isna(current_volume) or pd.isna(volume_ma) or volume_ma <= 0:
        return False

    return current_volume > volume_ma


def detect_price_action_signal(df_5m: pd.DataFrame, direction: str):
    """
    เงื่อนไขที่ 3: ตรวจแท่งเทียนล่าสุดบน 5M ว่าเป็นแท่งยืนยันการกลับตัว (Rejection/Pin Bar)
    ตามทิศทางที่กำหนดหรือไม่ (เรียกใช้หลังผ่านเงื่อนไข Trend + Support/Resistance แล้ว)

    BUY  : แท่งต้องเป็นขาขึ้น (Close > Open) และไส้ล่างยาว >= ขนาดตัวแท่ง
           (แรงซื้อดันราคากลับขึ้นจากแนวรับ)
    SELL : แท่งต้องเป็นขาลง (Close < Open) และไส้บนยาว >= ขนาดตัวแท่ง
           (แรงขายดันราคากลับลงจากแนวต้าน)

    (ใหม่ v4) เพิ่มเงื่อนไขย่อย 2 ข้อก่อนยืนยันสัญญาณ:
    - Volume ของแท่งนี้ต้องสูงกว่าค่าเฉลี่ย Volume ย้อนหลัง (VOLUME_MA_PERIOD แท่ง)
    - ต้องคำนวณ ATR(ATR_PERIOD) ได้ เพื่อใช้กำหนด SL แบบ Dynamic (SL = ATR_SL_MULTIPLIER x ATR)

    คืนค่า dict สัญญาณพร้อม Entry/SL/TP/ATR หรือ None ถ้าแท่งไม่ยืนยันครบทุกเงื่อนไข
    """
    if df_5m.empty:
        return None

    current = df_5m.iloc[-1]
    candle_time = df_5m.index[-1]

    open_ = float(current["Open"])
    high_ = float(current["High"])
    low_ = float(current["Low"])
    close_ = float(current["Close"])

    body = abs(close_ - open_)
    upper_wick = high_ - max(open_, close_)
    lower_wick = min(open_, close_) - low_

    is_bullish_pin = direction == "BUY" and close_ > open_ and lower_wick >= body
    is_bearish_pin = direction == "SELL" and close_ < open_ and upper_wick >= body

    if not (is_bullish_pin or is_bearish_pin):
        return None

    # ----- (ใหม่ v4) เงื่อนไข Volume: แท่งยืนยันต้องมี Volume สูงกว่าค่าเฉลี่ย 20 แท่ง -----
    if not check_volume_confirmation(df_5m, VOLUME_MA_PERIOD):
        log.info("แท่งยืนยัน %s มี Volume ไม่สูงพอ (ต่ำกว่า MA%d) จึงไม่ยิงสัญญาณ", direction, VOLUME_MA_PERIOD)
        return None

    # ----- (ใหม่ v4) คำนวณ ATR สำหรับ Dynamic SL -----
    atr_value = calculate_atr(df_5m, ATR_PERIOD)
    if atr_value is None:
        return None

    if is_bullish_pin:
        entry = close_
        sl = low_ - (ATR_SL_MULTIPLIER * atr_value)
        risk = entry - sl
        if risk <= 0:
            return None
        tp = entry + (risk * RISK_REWARD_RATIO)

        return {
            "direction": "BUY",
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "pattern": "Bullish Rejection (Pin Bar)",
            "candle_time": candle_time,
            "atr": atr_value,
        }

    # is_bearish_pin
    entry = close_
    sl = high_ + (ATR_SL_MULTIPLIER * atr_value)
    risk = sl - entry
    if risk <= 0:
        return None
    tp = entry - (risk * RISK_REWARD_RATIO)

    return {
        "direction": "SELL",
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "pattern": "Bearish Rejection (Pin Bar)",
        "candle_time": candle_time,
        "atr": atr_value,
    }


# ---------------------------------------------------------------------------
# เงื่อนไขที่ 2: Support/Resistance บน 15M (Swing High/Low Fractal)
# ---------------------------------------------------------------------------
def find_support_resistance(df_15m: pd.DataFrame, window: int = SR_FRACTAL_WINDOW):
    """
    หาแนวรับ (support) และแนวต้าน (resistance) แบบ Price Action ด้วยวิธี Fractal:
    - จุดใดจุดหนึ่งจะถูกนับเป็น "แนวต้าน" ถ้า High ของมันสูงที่สุดเมื่อเทียบกับ
      แท่งซ้าย/ขวาจำนวน `window` แท่ง (สวิงไฮ)
    - จะถูกนับเป็น "แนวรับ" ถ้า Low ของมันต่ำที่สุดเมื่อเทียบกับแท่งซ้าย/ขวา (สวิงโลว์)

    คืนค่า (supports: list[float], resistances: list[float])
    """
    highs = df_15m["High"].values
    lows = df_15m["Low"].values
    n = len(df_15m)

    supports = []
    resistances = []

    for i in range(window, n - window):
        window_high = highs[i - window : i + window + 1]
        window_low = lows[i - window : i + window + 1]

        if highs[i] == window_high.max():
            resistances.append(float(highs[i]))

        if lows[i] == window_low.min():
            supports.append(float(lows[i]))

    return supports, resistances


def find_nearest_level(price: float, levels: list, tolerance: float):
    """
    หาแนวรับ/แนวต้านที่ใกล้ราคาปัจจุบันที่สุด ถ้าอยู่ในระยะ `tolerance` USD
    คืนค่าระดับราคานั้น หรือ None ถ้าไม่มีระดับไหนอยู่ใกล้พอ
    """
    if not levels:
        return None

    nearest = min(levels, key=lambda lvl: abs(lvl - price))
    if abs(nearest - price) <= tolerance:
        return nearest
    return None


def check_pa_confluence(direction: str, entry_price: float, df_15m: pd.DataFrame):
    """
    เงื่อนไขที่ 2: ราคาต้องอยู่ใกล้แนวรับ (BUY) หรือแนวต้าน (SELL) บน 15M
    ภายในระยะ SR_TOLERANCE_USD ถึงจะถือว่าผ่านเงื่อนไข

    คืนค่า matched_level (float) ถ้าผ่าน, หรือ None ถ้าไม่ผ่าน
    """
    if df_15m is None or df_15m.empty or len(df_15m) < (SR_FRACTAL_WINDOW * 2 + 1):
        log.warning("ข้อมูล 15M ไม่พอสำหรับหาแนวรับ-แนวต้าน")
        return None

    supports, resistances = find_support_resistance(df_15m, window=SR_FRACTAL_WINDOW)

    if direction == "BUY":
        return find_nearest_level(entry_price, supports, SR_TOLERANCE_USD)
    elif direction == "SELL":
        return find_nearest_level(entry_price, resistances, SR_TOLERANCE_USD)

    return None


# ---------------------------------------------------------------------------
# SIGNAL MONITORING — เช็คว่าสัญญาณที่เปิดค้างไว้ชน TP/SL หรือยัง
# ---------------------------------------------------------------------------
def monitor_active_signals(df_5m: pd.DataFrame) -> None:
    """
    ใช้ High/Low ของแท่ง 5M ล่าสุดมาเช็คกับทุกสัญญาณใน active_signals
    - BUY : Low <= SL  -> โดน SL (Loss) | High >= TP -> โดน TP (Win)
    - SELL: High >= SL -> โดน SL (Loss) | Low <= TP  -> โดน TP (Win)

    ถ้าราคาแกว่งชนทั้ง TP และ SL ในแท่งเดียวกัน จะยึด SL เป็นหลักก่อน (conservative)
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
                msg = format_result_message(signal, hit_result, hit_price)
                send_telegram_message(msg)

                if hit_result == "TP":
                    weekly_stats["wins"] += 1
                    log.info("สัญญาณ id=%s (%s) ชน TP", signal["id"], direction)
                else:
                    weekly_stats["losses"] += 1
                    log.info("สัญญาณ id=%s (%s) ชน SL", signal["id"], direction)
            else:
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
                else:
                    still_active.append(signal)

        active_signals[:] = still_active


# ---------------------------------------------------------------------------
# WEEKLY SUMMARY — ส่งสรุปทุกวันศุกร์ 23:50 UTC
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

    if _last_weekly_summary_sent_isoweek == current_key:
        return

    msg = format_weekly_summary_message()
    send_telegram_message(msg)
    log.info("ส่งสรุปผลประจำสัปดาห์แล้ว: %s", weekly_stats)

    with state_lock:
        weekly_stats["signals"] = 0
        weekly_stats["wins"] = 0
        weekly_stats["losses"] = 0
        weekly_stats["timeouts"] = 0

    _last_weekly_summary_sent_isoweek = current_key


# ---------------------------------------------------------------------------
# (ใหม่ v4) TIME SESSION FILTER — เปิดสัญญาณใหม่เฉพาะช่วง London/New York
# ---------------------------------------------------------------------------
def is_within_trading_session() -> bool:
    """
    เช็คว่าตอนนี้อยู่ในช่วงเวลาเทรดที่กำหนดหรือไม่ (UTC 07:00 - 16:00 = ไทย 14:00 - 23:00)
    ใช้กรองเฉพาะตอน "เปิดสัญญาณใหม่" เท่านั้น — การ monitor สัญญาณเดิมและสรุปผลรายสัปดาห์
    ยังคงทำงาน 24/7 ตามปกติไม่ว่าจะอยู่ในช่วงนี้หรือไม่
    """
    now_hour = datetime.now(timezone.utc).hour
    return SESSION_START_HOUR_UTC <= now_hour < SESSION_END_HOUR_UTC


# ---------------------------------------------------------------------------
# MAIN SCAN LOOP
# ---------------------------------------------------------------------------
def scan_once():
    try:
        df_h1 = fetch_h1_data()
        trend = get_h1_trend(df_h1)
        log.info("H1 Trend (เงื่อนไข 1): %s", trend)

        df_5m = fetch_5m_data()

        # ทุกรอบต้องเช็คสัญญาณที่ค้างอยู่ก่อน ไม่ว่าจะมีเทรนใหม่หรือไม่
        monitor_active_signals(df_5m)

        # เช็คว่าถึงเวลาส่งสรุปประจำสัปดาห์หรือยัง (ทำทุกรอบ ไม่ผูกกับเทรน)
        check_and_send_weekly_summary()

        if trend == "NONE":
            return

        # ----- (ใหม่ v4) Time Session Filter: เปิดสัญญาณใหม่เฉพาะช่วง London/NY -----
        if not is_within_trading_session():
            log.info(
                "นอกช่วงเวลาเทรด (UTC %02d:00-%02d:00) จึงไม่เปิดสัญญาณใหม่ (ยัง monitor สัญญาณเดิมตามปกติ)",
                SESSION_START_HOUR_UTC, SESSION_END_HOUR_UTC,
            )
            return

        direction_candidate = "BUY" if trend == "UP" else "SELL"

        # ----- เงื่อนไขที่ 2: ราคาปัจจุบันต้องอยู่ใกล้ Support/Resistance บน 15M -----
        df_15m = fetch_15m_data()
        latest_close = float(df_5m["Close"].iloc[-1])
        pa_level = check_pa_confluence(direction_candidate, latest_close, df_15m)

        if pa_level is None:
            log.info(
                "ราคาไม่อยู่ใกล้แนวรับ/แนวต้านบน 15M ภายใน %.1f USD (เงื่อนไข 2 ไม่ผ่าน) ข้าม Trend=%s",
                SR_TOLERANCE_USD, direction_candidate,
            )
            return

        # ----- เงื่อนไขที่ 3: แท่งยืนยัน Price Action + Volume บน 5M -----
        signal = detect_price_action_signal(df_5m, direction_candidate)

        if signal is None:
            log.info("ไม่พบแท่งยืนยัน Price Action/Volume ในรอบนี้ (เงื่อนไข 3 ไม่ผ่าน, Trend=%s)", direction_candidate)
            return

        candle_time = signal["candle_time"]

        # ----- กันยิงสัญญาณซ้ำในแท่งเทียนเดิม -----
        if (
            last_signal_state["candle_time"] == candle_time
            and last_signal_state["direction"] == signal["direction"]
        ):
            log.info("สัญญาณ %s ที่แท่ง %s เคยยิงไปแล้ว ข้ามรอบนี้", signal["direction"], candle_time)
            return

        # ----- ผ่านครบ 4 เงื่อนไข: ยิงสัญญาณ -----
        msg = format_signal_message(
            direction=signal["direction"],
            entry=signal["entry"],
            sl=signal["sl"],
            tp=signal["tp"],
            pattern=signal["pattern"],
            candle_time=candle_time,
            pa_level=pa_level,
            atr_value=signal["atr"],
        )
        send_telegram_message(msg)
        log.info(
            "ส่งสัญญาณ %s สำเร็จ (Entry=%.2f SL=%.2f TP=%.2f PA=%.2f ATR=%.2f)",
            signal["direction"], signal["entry"], signal["sl"], signal["tp"], pa_level, signal["atr"],
        )

        last_signal_state["candle_time"] = candle_time
        last_signal_state["direction"] = signal["direction"]

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
