import os
import logging
import requests
import google.generativeai as genai
from flask import Flask, request, jsonify

# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("trading-assistant-bot")

# --------------------------------------------------------------------------
# Environment variables
# --------------------------------------------------------------------------
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

if not TELEGRAM_TOKEN:
    logger.warning("TELEGRAM_TOKEN is not set. The bot will not be able to talk to Telegram.")
if not GEMINI_API_KEY:
    logger.warning("GEMINI_API_KEY is not set. Gemini calls will fail.")

TELEGRAM_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
TELEGRAM_FILE_BASE = f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}"

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

GEMINI_MODEL_NAME = "gemini-1.5-flash"

SYSTEM_PROMPT = (
    "You are an expert Price Action & Technical Analysis Trading Assistant for "
    "XAUUSD (Gold). Analyze the provided chart screenshot thoroughly. Provide a "
    "concise, structured analysis in Thai covering: "
    "1) Current Market Structure / Trend "
    "2) Key Candlestick Patterns & Price Action "
    "3) Actionable Advice (Should enter now or wait for which candle close?) "
    "4) Suggested Setup parameters (Entry, SL, TP with Risk-to-Reward ratio) if applicable. "
    "Always include a short disclaimer at the end that this is not financial advice "
    "and the user should manage their own risk."
)

app = Flask(__name__)


# --------------------------------------------------------------------------
# Telegram helper functions
# --------------------------------------------------------------------------
def telegram_get(method, params=None, timeout=15):
    url = f"{TELEGRAM_API_BASE}/{method}"
    resp = requests.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def telegram_post(method, payload=None, timeout=30):
    url = f"{TELEGRAM_API_BASE}/{method}"
    resp = requests.post(url, json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def send_message(chat_id, text, parse_mode="Markdown"):
    """Send a text message to a Telegram chat, chunking if it exceeds Telegram's limit."""
    max_len = 4000  # stay under Telegram's 4096 char limit with some buffer
    chunks = [text[i:i + max_len] for i in range(0, len(text), max_len)] or [""]

    for chunk in chunks:
        payload = {
            "chat_id": chat_id,
            "text": chunk,
            "parse_mode": parse_mode,
        }
        try:
            telegram_post("sendMessage", payload)
        except requests.exceptions.HTTPError as exc:
            # Markdown parsing can fail on malformed content from the model;
            # retry once as plain text so the user still gets a reply.
            logger.warning("sendMessage with parse_mode failed (%s), retrying as plain text", exc)
            telegram_post("sendMessage", {"chat_id": chat_id, "text": chunk})


def send_chat_action(chat_id, action="typing"):
    try:
        telegram_post("sendChatAction", {"chat_id": chat_id, "action": action})
    except Exception as exc:  # noqa: BLE001
        logger.warning("sendChatAction failed: %s", exc)


def get_file_path(file_id):
    data = telegram_get("getFile", {"file_id": file_id})
    return data["result"]["file_path"]


def download_telegram_file(file_path):
    url = f"{TELEGRAM_FILE_BASE}/{file_path}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.content


def get_highest_resolution_photo(photo_array):
    """Telegram sends multiple sizes; the last entry is the highest resolution."""
    return max(photo_array, key=lambda p: p.get("file_size", 0) or p.get("width", 0))


# --------------------------------------------------------------------------
# Gemini analysis
# --------------------------------------------------------------------------
def analyze_chart_with_gemini(image_bytes, mime_type="image/jpeg"):
    model = genai.GenerativeModel(
        model_name=GEMINI_MODEL_NAME,
        system_instruction=SYSTEM_PROMPT,
    )

    image_part = {"mime_type": mime_type, "data": image_bytes}
    user_prompt = (
        "นี่คือภาพหน้าจอกราฟ XAUUSD ล่าสุด กรุณาวิเคราะห์ตามโครงสร้างที่กำหนดไว้"
    )

    response = model.generate_content(
        [user_prompt, image_part],
        generation_config={
            "temperature": 0.4,
            "max_output_tokens": 1024,
        },
    )
    return response.text


# --------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------
@app.route("/", methods=["GET"])
def index():
    return "Trading Assistant Bot is running!"


@app.route("/telegram-webhook", methods=["POST"])
def telegram_webhook():
    update = request.get_json(silent=True) or {}
    logger.info("Received update: %s", update.get("update_id"))

    message = update.get("message") or update.get("edited_message")
    if not message:
        return jsonify(ok=True)

    chat_id = message["chat"]["id"]

    try:
        photo_array = message.get("photo")

        if photo_array:
            send_chat_action(chat_id, "typing")

            best_photo = get_highest_resolution_photo(photo_array)
            file_path = get_file_path(best_photo["file_id"])
            image_bytes = download_telegram_file(file_path)

            mime_type = "image/png" if file_path.lower().endswith(".png") else "image/jpeg"

            analysis_text = analyze_chart_with_gemini(image_bytes, mime_type=mime_type)
            send_message(chat_id, analysis_text)

        elif message.get("text") in ("/start", "/help"):
            send_message(
                chat_id,
                "สวัสดีครับ ส่งภาพหน้าจอกราฟ XAUUSD มาได้เลย "
                "ผมจะวิเคราะห์ Price Action และ Market Structure ให้ครับ 📊",
                parse_mode=None,
            )
        else:
            send_message(
                chat_id,
                "กรุณาส่งภาพหน้าจอกราฟ XAUUSD (Gold) เพื่อให้ผมวิเคราะห์ครับ",
                parse_mode=None,
            )

    except Exception as exc:  # noqa: BLE001
        logger.exception("Error handling update")
        try:
            send_message(
                chat_id,
                "ขออภัยครับ เกิดข้อผิดพลาดระหว่างวิเคราะห์ภาพ กรุณาลองใหม่อีกครั้ง",
                parse_mode=None,
            )
        except Exception:  # noqa: BLE001
            pass

    return jsonify(ok=True)


@app.route("/set-webhook", methods=["GET"])
def set_webhook():
    """
    Convenience endpoint to (re)register the Telegram webhook.
    Call once: https://<your-render-url>/set-webhook?url=https://<your-render-url>/telegram-webhook
    """
    target_url = request.args.get("url")
    if not target_url:
        return jsonify(ok=False, error="Missing 'url' query parameter"), 400

    result = telegram_post("setWebhook", {"url": target_url})
    return jsonify(result)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
