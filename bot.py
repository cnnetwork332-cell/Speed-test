import os
import html
import asyncio
import logging
import sqlite3
import statistics
from datetime import datetime, timezone
from functools import wraps
from concurrent.futures import ThreadPoolExecutor

import speedtest
from telegram import Update
from telegram.constants import ChatType
from telegram.error import TelegramError, RetryAfter
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ========================= CONFIGURATION =========================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
GROUP_ID_RAW = os.getenv("GROUP_ID", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("Please set BOT_TOKEN environment variable.")
if not GROUP_ID_RAW:
    raise RuntimeError("Please set GROUP_ID environment variable.")

GROUP_ID = int(GROUP_ID_RAW)
INTERVAL = max(60, int(os.getenv("INTERVAL", "1800")))  # seconds; minimum 60
MIN_DOWNLOAD_MBPS = float(os.getenv("MIN_DOWNLOAD_MBPS", "0"))
MIN_UPLOAD_MBPS = float(os.getenv("MIN_UPLOAD_MBPS", "0"))
ALERT_ON_FAILURE = os.getenv("ALERT_ON_FAILURE", "true").lower() == "true"
SHOW_CLIENT_IP = os.getenv("SHOW_CLIENT_IP", "false").lower() == "true"
SHOW_COORDINATES = os.getenv("SHOW_COORDINATES", "false").lower() == "true"
DB_PATH = os.getenv("DB_PATH", "speedtests.sqlite3")
HISTORY_LIMIT = max(1, min(20, int(os.getenv("HISTORY_LIMIT", "10"))))

# Comma-separated Telegram user IDs. If empty, commands are not restricted.
# Set ADMIN_IDS to secure monitor controls, e.g. "123456789,987654321".
ADMIN_IDS = {
    int(value.strip())
    for value in os.getenv("ADMIN_IDS", "").split(",")
    if value.strip().isdigit()
}

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("speedtest-bot")
# HTTP client INFO logs include Telegram API URLs (which contain the bot token).
# Keep those logs quiet to avoid leaking credentials into deployment logs.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# speedtest-cli is blocking; use one worker and one test at a time.
executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="speedtest")
test_lock = asyncio.Lock()


# ========================= DATABASE ==============================

def db_connect():
    conn = sqlite3.connect(DB_PATH, timeout=15)
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
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tests_timestamp ON tests(timestamp)")


def save_result(result):
    with db_connect() as conn:
        conn.execute("""
            INSERT INTO tests (
                timestamp, download_mbps, upload_mbps, ping_ms,
                bytes_sent_mb, bytes_received_mb, server_name,
                server_country, server_sponsor, client_ip, isp,
                client_country, share_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            result["timestamp"], result["download_mbps"],
            result["upload_mbps"], result["ping_ms"],
            result["bytes_sent_mb"], result["bytes_received_mb"],
            result["server_name"], result["server_country"],
            result["server_sponsor"], result["client_ip"],
            result["isp"], result["client_country"], result["share_url"],
        ))


def read_history(limit=HISTORY_LIMIT):
    with db_connect() as conn:
        return conn.execute(
            "SELECT * FROM tests ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()


def read_stats():
    with db_connect() as conn:
        return conn.execute("""
            SELECT COUNT(*) AS count,
                   AVG(download_mbps) AS avg_down,
                   MIN(download_mbps) AS min_down,
                   MAX(download_mbps) AS max_down,
                   AVG(upload_mbps) AS avg_up,
                   MIN(upload_mbps) AS min_up,
                   MAX(upload_mbps) AS max_up,
                   AVG(ping_ms) AS avg_ping,
                   MAX(timestamp) AS last_test
            FROM tests
        """).fetchone()


def read_latest():
    with db_connect() as conn:
        return conn.execute(
            "SELECT * FROM tests ORDER BY id DESC LIMIT 1"
        ).fetchone()


# ========================= SPEED TEST ============================

def bytes_to_mb(value):
    return round(value / (1024 * 1024), 2)


def run_speedtest_sync():
    """Blocking function. Always call it through asyncio.to_thread."""
    st = speedtest.Speedtest()
    st.get_best_server()

    download_bits = st.download()
    upload_bits = st.upload()
    data = st.results.dict()

    try:
        share_url = st.results.share()
    except Exception:
        log.exception("Could not create Speedtest share image")
        share_url = None

    server = data.get("server") or {}
    client = data.get("client") or {}

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "download_mbps": round(download_bits / 1_000_000, 2),
        "upload_mbps": round(upload_bits / 1_000_000, 2),
        "ping_ms": round(float(data.get("ping") or 0), 2),
        "bytes_sent_mb": bytes_to_mb(data.get("bytes_sent", 0)),
        "bytes_received_mb": bytes_to_mb(data.get("bytes_received", 0)),
        "server_name": str(server.get("name", "Unknown")),
        "server_country": str(server.get("country", "Unknown")),
        "server_sponsor": str(server.get("sponsor", "Unknown")),
        "server_lat": server.get("lat"),
        "server_lon": server.get("lon"),
        "client_ip": str(client.get("ip", "Unknown")),
        "isp": str(client.get("isp", "Unknown")),
        "client_country": str(client.get("country", "Unknown")),
        "client_lat": client.get("lat"),
        "client_lon": client.get("lon"),
        "share_url": share_url,
    }


# ========================= FORMATTING ============================

def fmt(value, digits=2):
    return "N/A" if value is None else f"{value:.{digits}f}"


def format_result(r, include_alert=True):
    """Return a premium, Telegram-HTML formatted speed-test report."""
    try:
        dt = datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        time_text = dt.strftime("%d %b %Y • %H:%M:%S UTC")
    except (ValueError, TypeError, AttributeError):
        time_text = html.escape(str(r.get("timestamp", "Unknown")))

    download = float(r.get("download_mbps") or 0)
    upload = float(r.get("upload_mbps") or 0)
    ping = float(r.get("ping_ms") or 0)
    if download >= 100 and ping <= 25:
        verdict, icon = "EXCELLENT", "🟢"
    elif download >= 50 and ping <= 50:
        verdict, icon = "VERY GOOD", "🔵"
    elif download >= 25 and ping <= 80:
        verdict, icon = "GOOD", "🟡"
    elif download >= 5:
        verdict, icon = "FAIR", "🟠"
    else:
        verdict, icon = "NEEDS ATTENTION", "🔴"

    def esc(value, fallback="Unknown"):
        return html.escape(str(value if value not in (None, "") else fallback))

    lines = [
        "<b>━━━━━━━━━━━━━━━━━━━━</b>",
        "<b>🚀  SPEEDTEST  /  PERFORMANCE REPORT</b>",
        "<b>━━━━━━━━━━━━━━━━━━━━</b>",
        f"{icon} <b>CONNECTION QUALITY: {verdict}</b>",
        "",
        "<b>⚡ SPEED METRICS</b>",
        f"⬇️  <b>Download</b>   <code>{download:,.2f} Mbps</code>",
        f"⬆️  <b>Upload</b>       <code>{upload:,.2f} Mbps</code>",
        f"🏓  <b>Ping</b>          <code>{ping:,.2f} ms</code>",
        "",
        "<b>🌐 NETWORK PROVIDER</b>",
        f"🏢  <b>ISP</b>       {esc(r.get('isp'))}",
        f"📍  <b>Country</b>   {esc(r.get('client_country'))}",
        "",
        "<b>🖥️ TEST SERVER</b>",
        f"📡  <b>Name</b>      {esc(r.get('server_name'))}",
        f"🌍  <b>Country</b>   {esc(r.get('server_country'))}",
        f"🤝  <b>Sponsor</b>   {esc(r.get('server_sponsor'))}",
        "",
        "<b>📦 TEST DATA</b>",
        f"⬆️  <b>Sent</b>       <code>{float(r.get('bytes_sent_mb') or 0):,.2f} MB</code>",
        f"⬇️  <b>Received</b>   <code>{float(r.get('bytes_received_mb') or 0):,.2f} MB</code>",
        "",
        "<b>🕒 TEST TIMESTAMP</b>",
        f"<code>{time_text}</code>",
    ]
    if SHOW_CLIENT_IP:
        lines += [f"🌐 <b>Public IP</b>  <code>{esc(r.get('client_ip'))}</code>"]
    if SHOW_COORDINATES:
        lines += [
            "", "<b>📍 COORDINATES</b>",
            f"Server: <code>{esc(r.get('server_lat'))}, {esc(r.get('server_lon'))}</code>",
            f"Client: <code>{esc(r.get('client_lat'))}, {esc(r.get('client_lon'))}</code>",
        ]

    alerts = []
    if include_alert and MIN_DOWNLOAD_MBPS > 0 and download < MIN_DOWNLOAD_MBPS:
        alerts.append(f"Download is below the configured target of {MIN_DOWNLOAD_MBPS:g} Mbps.")
    if include_alert and MIN_UPLOAD_MBPS > 0 and upload < MIN_UPLOAD_MBPS:
        alerts.append(f"Upload is below the configured target of {MIN_UPLOAD_MBPS:g} Mbps.")
    if alerts:
        lines += ["", "<b>⚠️ PERFORMANCE ALERT</b>"]
        lines += [f"• {html.escape(item)}" for item in alerts]

    if r.get("share_url"):
        lines += ["", f'🔗 <a href="{html.escape(str(r["share_url"]), quote=True)}">View Speedtest share result</a>']
    lines += ["", "<i>Automated network monitoring • Powered by Speedtest Bot</i>", "<b>━━━━━━━━━━━━━━━━━━━━</b>"]
    return "\n".join(lines)


async def safe_send(context, chat_id, text):
    """Send a message with basic Telegram flood-control handling."""
    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            disable_web_page_preview=True,
        )
    except RetryAfter as exc:
        delay = getattr(exc, "retry_after", 5)
        if hasattr(delay, "total_seconds"):
            delay = delay.total_seconds()
        await asyncio.sleep(float(delay) + 1)
        try:
            await context.bot.send_message(
                chat_id=chat_id, text=text, disable_web_page_preview=True
            )
        except TelegramError:
            log.exception("Telegram send failed after retry")
    except TelegramError:
        log.exception("Telegram send failed")


async def post_result(context, result):
    """Publish the full report as formatted text; avoids flaky remote image uploads."""
    caption = format_result(result)
    try:
        await context.bot.send_message(
            chat_id=GROUP_ID,
            text=caption,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except RetryAfter as exc:
        delay = getattr(exc, "retry_after", 5)
        if hasattr(delay, "total_seconds"):
            delay = delay.total_seconds()
        await asyncio.sleep(float(delay) + 1)
        await context.bot.send_message(
            chat_id=GROUP_ID,
            text=caption,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except TelegramError:
        log.exception("Could not publish speed-test report; verify GROUP_ID and bot permissions")


# ========================= ACCESS CONTROL ========================

def admin_only(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if ADMIN_IDS and (user is None or user.id not in ADMIN_IDS):
            if update.effective_message:
                await update.effective_message.reply_text(
                    "⛔ You are not authorized to use this command."
                )
            return
        return await func(update, context)
    return wrapper


# ========================= CORE OPERATIONS ======================

async def perform_test(context, notify_group=True):
    # Prevent manual commands and scheduled monitoring from overlapping.
    if test_lock.locked():
        return None, "A speed test is already running. Please try again shortly."

    async with test_lock:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(executor, run_speedtest_sync)
        await asyncio.to_thread(save_result, result)

    if notify_group:
        await post_result(context, result)
    return result, None


async def monitor_job(context: ContextTypes.DEFAULT_TYPE):
    if context.application.bot_data.get("monitor_paused", False):
        return

    try:
        result, error = await perform_test(context, notify_group=True)
        if error:
            log.info(error)
            return
        log.info(
            "Test saved: download=%.2f Mbps upload=%.2f Mbps ping=%.2f ms",
            result["download_mbps"], result["upload_mbps"], result["ping_ms"],
        )
    except Exception:
        log.exception("Scheduled speed test failed")
        if ALERT_ON_FAILURE:
            await safe_send(context, GROUP_ID, "❌ Scheduled speed test failed. Check the bot logs.")


# ========================= COMMANDS ==============================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "<b>🚀 SPEEDTEST MONITOR</b>\n"
        "<i>Your network. Measured, tracked, monitored.</i>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>📊 TEST & REPORTS</b>\n"
        "<code>/speedtest</code>  Run a live speed test\n"
        "<code>/status</code>      Monitor status + latest result\n"
        "<code>/stats</code>       Lifetime performance summary\n"
        "<code>/history</code>     Recent test history\n\n"
        "<b>⚙️ MONITOR CONTROLS</b>\n"
        "<code>/monitor_on</code>  Resume scheduled tests\n"
        "<code>/monitor_off</code> Pause scheduled tests\n"
        "<code>/setinterval 1800</code> Change interval in seconds\n\n"
        "<b>💡 QUICK NOTES</b>\n"
        "• Minimum interval: 60 seconds\n"
        "• Speeds are shown in Mbps\n"
        "• Reports are saved to the local history database\n\n"
        "<i>Built for reliable network monitoring.</i>"
    )
    await update.effective_message.reply_text(text, parse_mode="HTML", disable_web_page_preview=True)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)


@admin_only
async def speedtest_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.effective_message.reply_text(
        "⏳ <b>Running speed test…</b>\n<i>Measuring download, upload and latency. This may take a little while.</i>",
        parse_mode="HTML"
    )
    try:
        result, error = await perform_test(context, notify_group=False)
        if error:
            await msg.edit_text(f"⚠️ {error}")
            return
        await msg.edit_text(format_result(result), parse_mode="HTML", disable_web_page_preview=True)
        # Also publish the result in the configured group if command was elsewhere.
        if update.effective_chat and update.effective_chat.id != GROUP_ID:
            await post_result(context, result)
        elif update.effective_chat and update.effective_chat.id == GROUP_ID:
            # The result is already visible in the command chat.
            pass
    except Exception as exc:
        log.exception("Manual speed test failed")
        await msg.edit_text(f"❌ <b>Speed test failed</b>\n<code>{html.escape(str(exc)[:400])}</code>", parse_mode="HTML")


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    app = context.application
    paused = app.bot_data.get("monitor_paused", False)
    latest = await asyncio.to_thread(read_latest)
    state = "⏸ PAUSED" if paused else "🟢 ACTIVE"
    lines = [
        "📡 SPEEDTEST MONITOR | STATUS",
        "━━━━━━━━━━━━━━━━━━━━",
        f"State       {state}",
        f"Interval    {app.bot_data.get('interval', INTERVAL)} seconds",
    ]
    if latest:
        try:
            dt = datetime.fromisoformat(latest["timestamp"])
            last_text = dt.strftime("%d %b %Y • %H:%M UTC")
        except (ValueError, TypeError):
            last_text = str(latest["timestamp"])
        lines += ["", "📊 LATEST RESULT",
                  f"⬇️ Download   {latest['download_mbps']:,.2f} Mbps",
                  f"⬆️ Upload     {latest['upload_mbps']:,.2f} Mbps",
                  f"🏓 Latency    {latest['ping_ms']:,.2f} ms",
                  f"🕒 Tested     {last_text}"]
    else:
        lines += ["", "No test results have been recorded yet."]
    lines += ["", "━━━━━━━━━━━━━━━━━━━━"]
    await update.effective_message.reply_text("\n".join(lines))


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    row = await asyncio.to_thread(read_stats)
    if not row or row["count"] == 0:
        await update.effective_message.reply_text("No speed tests recorded yet.")
        return

    text = (
        "📊 SPEEDTEST | PERFORMANCE OVERVIEW\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"Tests recorded  {row['count']}\n"
        f"Last test      {row['last_test']}\n\n"
        "⬇️ DOWNLOAD SPEED\n"
        f"  Average  {fmt(row['avg_down'])} Mbps\n"
        f"  Minimum  {fmt(row['min_down'])} Mbps\n"
        f"  Maximum  {fmt(row['max_down'])} Mbps\n\n"
        "⬆️ UPLOAD SPEED\n"
        f"  Average  {fmt(row['avg_up'])} Mbps\n"
        f"  Minimum  {fmt(row['min_up'])} Mbps\n"
        f"  Maximum  {fmt(row['max_up'])} Mbps\n\n"
        f"🏓 Average latency  {fmt(row['avg_ping'])} ms\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )
    await update.effective_message.reply_text(text)


async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = await asyncio.to_thread(read_history, HISTORY_LIMIT)
    if not rows:
        await update.effective_message.reply_text("No history available yet.")
        return

    lines = ["🧾 SPEEDTEST | RECENT HISTORY", "━━━━━━━━━━━━━━━━━━━━", f"Showing the latest {len(rows)} test(s)", ""]
    for row in rows:
        lines.append(
            f"🕒 {row['timestamp']}\n"
            f"⬇️ {row['download_mbps']:,.2f} Mbps   "
            f"⬆️ {row['upload_mbps']:,.2f} Mbps\n"
            f"🏓 Latency: {row['ping_ms']:,.2f} ms"
        )
    # Telegram message limit is 4096 characters.
    text = "\n\n".join(lines)
    await update.effective_message.reply_text(text[:4000])


@admin_only
async def monitor_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.application.bot_data["monitor_paused"] = True
    await update.effective_message.reply_text("⏸ Automatic speed tests paused.")


@admin_only
async def monitor_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.application.bot_data["monitor_paused"] = False
    await update.effective_message.reply_text("🟢 Automatic speed tests resumed.")


@admin_only
async def set_interval(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.effective_message.reply_text(
            f"Current interval: {context.application.bot_data.get('interval', INTERVAL)} seconds.\n"
            "Usage: /setinterval 1800 (minimum 60 seconds)"
        )
        return

    try:
        seconds = int(context.args[0])
        if seconds < 60 or seconds > 604800:
            raise ValueError
    except ValueError:
        await update.effective_message.reply_text(
            "❌ Enter a whole number from 60 to 604800 seconds."
        )
        return

    context.application.bot_data["interval"] = seconds
    job = context.application.bot_data.get("monitor_job")
    if job:
        job.schedule_removal()

    new_job = context.application.job_queue.run_repeating(
        monitor_job, interval=seconds, first=seconds, name="speedtest-monitor"
    )
    context.application.bot_data["monitor_job"] = new_job
    await update.effective_message.reply_text(
        f"✅ Monitoring interval changed to {seconds} seconds.\n"
        "This setting lasts until the bot restarts; set INTERVAL in the environment to persist it."
    )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.error(
        "Unhandled Telegram handler error",
        exc_info=(type(context.error), context.error, context.error.__traceback__)
        if context.error else None,
    )


# ========================= LIFECYCLE =============================

async def post_init(application: Application):
    await asyncio.to_thread(init_db)
    application.bot_data["monitor_paused"] = False
    application.bot_data["interval"] = INTERVAL

    job_queue = application.job_queue
    if job_queue is None:
        raise RuntimeError(
            'JobQueue unavailable. Install with: pip install "python-telegram-bot[job-queue]>=21,<23"'
        )

    job = job_queue.run_repeating(
        monitor_job,
        interval=INTERVAL,
        first=10,
        name="speedtest-monitor",
    )
    application.bot_data["monitor_job"] = job

    try:
        await application.bot.send_message(
            chat_id=GROUP_ID,
            text=(
                "🚀 SPEEDTEST MONITOR IS ONLINE\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "🟢 Status: Operational\n"
                f"⏱ Auto-test interval: {INTERVAL} seconds\n\n"
                "Use /help to explore reports and controls.\n"
                "━━━━━━━━━━━━━━━━━━━━"
            ),
        )
    except TelegramError:
        log.exception("Could not send startup message; check GROUP_ID and bot permissions")


def main():
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("speedtest", speedtest_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("history", history_command))
    application.add_handler(CommandHandler("monitor_off", monitor_off))
    application.add_handler(CommandHandler("monitor_on", monitor_on))
    application.add_handler(CommandHandler("setinterval", set_interval))
    application.add_error_handler(error_handler)

    log.info("Starting Speedtest Bot")
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    main()
