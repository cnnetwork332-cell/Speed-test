import os
import html
import asyncio
import logging
import sqlite3
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import speedtest
from telegram import Update
from telegram.constants import ParseMode
from telegram.error import TelegramError, RetryAfter
from telegram.ext import Application, ContextTypes


# ============================================================
# CONFIGURATION
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
GROUP_ID_RAW = os.getenv("GROUP_ID", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN environment variable.")

if not GROUP_ID_RAW:
    raise RuntimeError("Missing GROUP_ID environment variable.")

try:
    GROUP_ID = int(GROUP_ID_RAW)
    INTERVAL = int(os.getenv("INTERVAL", "1800"))
    MIN_DOWNLOAD_MBPS = float(os.getenv("MIN_DOWNLOAD_MBPS", "0"))
    MIN_UPLOAD_MBPS = float(os.getenv("MIN_UPLOAD_MBPS", "0"))
except ValueError as exc:
    raise RuntimeError("Invalid numeric environment variable.") from exc

if INTERVAL < 60 or INTERVAL > 604800:
    raise RuntimeError("INTERVAL must be between 60 and 604800 seconds.")

if MIN_DOWNLOAD_MBPS < 0 or MIN_UPLOAD_MBPS < 0:
    raise RuntimeError("Speed thresholds cannot be negative.")

ALERT_ON_FAILURE = (
    os.getenv("ALERT_ON_FAILURE", "true").lower() == "true"
)
SHOW_CLIENT_IP = (
    os.getenv("SHOW_CLIENT_IP", "false").lower() == "true"
)
DB_PATH = os.getenv("DB_PATH", "speedtests.sqlite3")

# One worker ensures that blocking speed tests never run concurrently.
executor = ThreadPoolExecutor(
    max_workers=1,
    thread_name_prefix="speedtest",
)

test_lock = asyncio.Lock()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("speedtest-bot")

# Avoid exposing bot-token URLs in HTTP client logs.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


# ============================================================
# DATABASE
# ============================================================

def db_connect():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db_connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                download_mbps REAL NOT NULL,
                upload_mbps REAL NOT NULL,
                ping_ms REAL NOT NULL,
                bytes_sent_mb REAL NOT NULL,
                bytes_received_mb REAL NOT NULL,
                server_name TEXT,
                server_country TEXT,
                server_sponsor TEXT,
                client_ip TEXT,
                isp TEXT,
                client_country TEXT,
                share_url TEXT
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tests_timestamp
            ON tests(timestamp)
        """)


def save_result(r):
    with db_connect() as conn:
        conn.execute("""
            INSERT INTO tests (
                timestamp, download_mbps, upload_mbps, ping_ms,
                bytes_sent_mb, bytes_received_mb, server_name,
                server_country, server_sponsor, client_ip, isp,
                client_country, share_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            r["timestamp"],
            r["download_mbps"],
            r["upload_mbps"],
            r["ping_ms"],
            r["bytes_sent_mb"],
            r["bytes_received_mb"],
            r["server_name"],
            r["server_country"],
            r["server_sponsor"],
            r["client_ip"],
            r["isp"],
            r["client_country"],
            r["share_url"],
        ))


# ============================================================
# SPEED TEST ENGINE
# ============================================================

def bytes_to_mb(value):
    return round(float(value or 0) / (1024 * 1024), 2)


def run_speedtest_sync():
    """Blocking Speedtest operation. Run only in the executor."""
    st = speedtest.Speedtest()
    st.get_best_server()

    download_bits = st.download()
    upload_bits = st.upload()
    data = st.results.dict()

    share_url = None
    try:
        share_url = st.results.share()
    except Exception:
        log.warning("Speedtest share link could not be generated.")

    server = data.get("server") or {}
    client = data.get("client") or {}

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(
            timespec="seconds"
        ),
        "download_mbps": round(download_bits / 1_000_000, 2),
        "upload_mbps": round(upload_bits / 1_000_000, 2),
        "ping_ms": round(float(data.get("ping") or 0), 2),
        "bytes_sent_mb": bytes_to_mb(data.get("bytes_sent")),
        "bytes_received_mb": bytes_to_mb(data.get("bytes_received")),
        "server_name": str(server.get("name") or "Unknown"),
        "server_country": str(server.get("country") or "Unknown"),
        "server_sponsor": str(server.get("sponsor") or "Unknown"),
        "client_ip": str(client.get("ip") or "Unknown"),
        "isp": str(client.get("isp") or "Unknown"),
        "client_country": str(client.get("country") or "Unknown"),
        "share_url": share_url,
    }


# ============================================================
# PREMIUM TELEGRAM REPORT
# ============================================================

def esc(value, fallback="Unknown"):
    if value is None or value == "":
        value = fallback
    return html.escape(str(value))


def get_quality(download, upload, ping):
    # A simple heuristic, not an official network quality rating.
    if download >= 100 and upload >= 30 and ping <= 25:
        return "EXCELLENT", "🟢"
    if download >= 50 and upload >= 10 and ping <= 50:
        return "VERY GOOD", "🔵"
    if download >= 25 and upload >= 5 and ping <= 80:
        return "GOOD", "🟡"
    if download >= 5:
        return "FAIR", "🟠"
    return "NEEDS ATTENTION", "🔴"


def format_result(r):
    download = float(r["download_mbps"])
    upload = float(r["upload_mbps"])
    ping = float(r["ping_ms"])

    quality, quality_icon = get_quality(download, upload, ping)

    try:
        dt = datetime.fromisoformat(
            r["timestamp"].replace("Z", "+00:00")
        )
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        time_text = dt.strftime("%d %b %Y  •  %H:%M:%S UTC")
    except (ValueError, TypeError):
        time_text = str(r.get("timestamp", "Unknown"))

    alerts = []

    if MIN_DOWNLOAD_MBPS > 0 and download < MIN_DOWNLOAD_MBPS:
        alerts.append(
            f"Download below target: {MIN_DOWNLOAD_MBPS:g} Mbps"
        )

    if MIN_UPLOAD_MBPS > 0 and upload < MIN_UPLOAD_MBPS:
        alerts.append(
            f"Upload below target: {MIN_UPLOAD_MBPS:g} Mbps"
        )

    lines = [
        "━━━━━━━━━━━━━━━━━━━━━━",
        "🚀 <b>SPEEDTEST</b> <i>• NETWORK MONITOR</i>",
        "━━━━━━━━━━━━━━━━━━━━━━",
        "",
        f"{quality_icon} <b>CONNECTION: {quality}</b>",
        "",
        "⚡ <b>SPEED PERFORMANCE</b>",
        "",
        f"⬇️ Download   <code>{download:,.2f} Mbps</code>",
        f"⬆️ Upload       <code>{upload:,.2f} Mbps</code>",
        f"🏓 Latency      <code>{ping:,.2f} ms</code>",
        "",
        "━━━━━━━━━━━━━━━━━━━━━━",
        "🌐 <b>NETWORK DETAILS</b>",
        "",
        f"🏢 Provider  •  {esc(r.get('isp'))}",
        f"📍 Country   •  {esc(r.get('client_country'))}",
        "",
        "🖥️ <b>TEST SERVER</b>",
        "",
        f"📡 Server    •  {esc(r.get('server_name'))}",
        f"🌍 Location  •  {esc(r.get('server_country'))}",
        f"🤝 Sponsor   •  {esc(r.get('server_sponsor'))}",
        "",
        "📦 <b>DATA TRANSFER</b>",
        "",
        f"⬆️ Sent       •  <code>{r['bytes_sent_mb']:,.2f} MB</code>",
        f"⬇️ Received   •  <code>{r['bytes_received_mb']:,.2f} MB</code>",
        "",
        "🕒 <b>TEST COMPLETED</b>",
        f"<code>{esc(time_text)}</code>",
    ]

    if SHOW_CLIENT_IP:
        lines += [
            "",
            f"🌐 Public IP  •  <code>{esc(r.get('client_ip'))}</code>",
        ]

    if alerts:
        lines += [
            "",
            "⚠️ <b>PERFORMANCE ALERT</b>",
        ]
        lines.extend(f"• {esc(alert)}" for alert in alerts)

    share_url = r.get("share_url")
    if share_url and str(share_url).startswith(("https://", "http://")):
        safe_url = html.escape(str(share_url), quote=True)
        lines += [
            "",
            f'🔗 <a href="{safe_url}">View Speedtest result</a>',
        ]

    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━",
        "<i>Automated monitoring • Powered by Speedtest Bot</i>",
    ]

    return "\n".join(lines)


# ============================================================
# TELEGRAM DELIVERY
# ============================================================

async def send_message(context, text, parse_mode=None):
    """Send with one retry if Telegram applies flood control."""
    try:
        await context.bot.send_message(
            chat_id=GROUP_ID,
            text=text,
            parse_mode=parse_mode,
            disable_web_page_preview=True,
        )
    except RetryAfter as exc:
        delay = getattr(exc, "retry_after", 5)
        if hasattr(delay, "total_seconds"):
            delay = delay.total_seconds()

        await asyncio.sleep(float(delay) + 1)

        await context.bot.send_message(
            chat_id=GROUP_ID,
            text=text,
            parse_mode=parse_mode,
            disable_web_page_preview=True,
        )


async def post_result(context, result):
    report = format_result(result)
    await send_message(context, report, ParseMode.HTML)


# ============================================================
# AUTOMATIC MONITOR
# ============================================================

async def perform_test(context):
    if test_lock.locked():
        log.warning("Previous speed test is still running; skipping this cycle.")
        return

    async with test_lock:
        loop = asyncio.get_running_loop()

        result = await loop.run_in_executor(
            executor, run_speedtest_sync
        )

        # Save results without blocking Telegram's event loop.
        await asyncio.to_thread(save_result, result)

    log.info(
        "Speedtest complete | Download %.2f Mbps | Upload %.2f Mbps | "
        "Ping %.2f ms",
        result["download_mbps"],
        result["upload_mbps"],
        result["ping_ms"],
    )

    await post_result(context, result)


async def monitor_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        await perform_test(context)
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Automatic speed test failed.")

        if ALERT_ON_FAILURE:
            try:
                await send_message(
                    context,
                    "⚠️ <b>SPEEDTEST FAILED</b>\n\n"
                    "The automatic network test could not be completed.\n"
                    "The monitor will try again at its next scheduled run.",
                    ParseMode.HTML,
                )
            except Exception:
                log.exception("Could not send the failure notification.")


# ============================================================
# STARTUP
# ============================================================

async def post_init(application: Application):
    await asyncio.to_thread(init_db)

    job_queue = application.job_queue
    if job_queue is None:
        raise RuntimeError(
            'Install JobQueue support: '
            'pip install "python-telegram-bot[job-queue]>=20.6,<23"'
        )

    application.bot_data["monitor_job"] = job_queue.run_repeating(
        monitor_job,
        interval=INTERVAL,
        first=10,
        name="automatic-speedtest",
    )

    log.info("Speedtest monitor started. Interval: %s seconds", INTERVAL)

    try:
        await application.bot.send_message(
            chat_id=GROUP_ID,
            text=(
                "🚀 <b>SPEEDTEST MONITOR</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "🟢 <b>SYSTEM ONLINE</b>\n\n"
                "Automatic network monitoring is active.\n"
                f"⏱ Test interval: <b>{INTERVAL // 60} min"
                f"{' ' + str(INTERVAL % 60) + ' sec' if INTERVAL % 60 else ''}</b>\n"
                "📊 Detailed reports: Enabled\n"
                "💾 Local result history: Enabled\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "<i>First test is scheduled shortly.</i>"
            ),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except TelegramError:
        log.exception("Startup message failed. Check GROUP_ID and bot permissions.")


def main():
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    # Intentionally no CommandHandlers: the bot is automatic-only.
    log.info("Starting automatic Speedtest Bot.")

    try:
        application.run_polling(
            allowed_updates=[],
            drop_pending_updates=True,
        )
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    main()
