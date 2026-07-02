import os
import asyncio
import logging
import speedtest
from telegram import Bot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_ID = int(os.getenv("GROUP_ID"))
INTERVAL = int(os.getenv("INTERVAL", "30"))  # 5 min

bot = Bot(BOT_TOKEN)


def mbps(bits):
    return round(bits / 1000000, 2)


def mb(byte):
    return round(byte / 1024 / 1024, 2)


def flag(country):
    flags = {
        "India": "🇮🇳",
        "Singapore": "🇸🇬",
        "United States": "🇺🇸",
        "Japan": "🇯🇵",
        "Germany": "🇩🇪",
        "United Kingdom": "🇬🇧",
        "Canada": "🇨🇦",
        "France": "🇫🇷",
        "Australia": "🇦🇺"
    }
    return flags.get(country, "🌍")


def run_speedtest():
    st = speedtest.Speedtest()

    st.get_best_server()

    download = st.download()
    upload = st.upload()

    st.results.share()

    data = st.results.dict()

    return {
        "image": data["share"],
        "download": mbps(download),
        "upload": mbps(upload),
        "ping": round(data["ping"],2),
        "server": data["server"],
        "client": data["client"],
        "bytes_sent": mb(data["bytes_sent"]),
        "bytes_received": mb(data["bytes_received"]),
        "timestamp": data["timestamp"]
    }


async def send():

    result = run_speedtest()

    s = result["server"]
    c = result["client"]

    caption = f"""
╭─《 🚀 SPEEDTEST INFO 》
├ ⬆ Upload : {result['upload']} Mbps
├ ⬇ Download : {result['download']} Mbps
├ 📶 Ping : {result['ping']} ms
├ 🕒 Time : {result['timestamp']}
├ 📤 Data Sent : {result['bytes_sent']} MB
╰ 📥 Data Received : {result['bytes_received']} MB

╭─《 🌐 SERVER 》
├ 🌍 Name : {s['name']}
├ 🏳 {flag(s['country'])} {s['country']}
├ 🏢 Sponsor : {s['sponsor']}
├ 📍 Latitude : {s['lat']}
╰ 📍 Longitude : {s['lon']}

╭─《 👤 CLIENT 》
├ 🌐 IP : {c['ip']}
├ 📡 ISP : {c['isp']}
├ 🌍 Country : {flag(c['country'])} {c['country']}
├ 📍 Latitude : {c['lat']}
╰ 📍 Longitude : {c['lon']}

━━━━━━━━━━━━━━━━━━
⚡ Powered By @jioxt
"""

    await bot.send_photo(
        chat_id=GROUP_ID,
        photo=result["image"],
        caption=caption
    )
    async def monitor():

    await bot.send_message(
        chat_id=GROUP_ID,
        text="🚀 Speedtest Bot Started Successfully."
    )

    while True:

        try:
            await send()
            logging.info("Speedtest sent successfully")

        except Exception as e:
            logging.exception(e)

            try:
                await bot.send_message(
                    chat_id=GROUP_ID,
                    text=f"❌ Speedtest Failed\n\n{e}"
                )
            except:
                pass

        await asyncio.sleep(INTERVAL)


async def main():
    await monitor()


if __name__ == "__main__":
    asyncio.run(main())
