import os
import asyncio
import logging
import speedtest

from telegram import Bot
from telegram.request import HTTPXRequest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_ID = int(os.getenv("GROUP_ID"))
INTERVAL = int(os.getenv("INTERVAL", "20"))

request = HTTPXRequest(
    connect_timeout=30,
    read_timeout=60,
    write_timeout=60,
    pool_timeout=60
)

bot = Bot(
    token=BOT_TOKEN,
    request=request
)


def mbps(bits):
    return round(bits / 1000000, 2)


def mb(byte):
    return round(byte / 1024 / 1024, 2)
    def run_speedtest():
    try:
        st = speedtest.Speedtest()

        st.get_best_server()

        download = st.download()
        upload = st.upload()

        image = st.results.share()

        data = st.results.dict()

        return {
            "image": image,
            "download": mbps(download),
            "upload": mbps(upload),
            "ping": round(data["ping"], 3),
            "timestamp": data["timestamp"],
            "bytes_sent": mb(data["bytes_sent"]),
            "bytes_received": mb(data["bytes_received"]),
            "server": {
                "name": data["server"]["name"],
                "country": data["server"]["country"],
                "sponsor": data["server"]["sponsor"],
                "lat": data["server"]["lat"],
                "lon": data["server"]["lon"]
            },
            "client": {
                "ip": data["client"]["ip"],
                "isp": data["client"]["isp"],
                "country": data["client"]["country"],
                "lat": data["client"]["lat"],
                "lon": data["client"]["lon"]
            }
        }

    except Exception as e:
        logging.exception(e)
        return None
        async def send():

    result = run_speedtest()

    if result is None:
        return

    s = result["server"]
    c = result["client"]

    caption = f"""╭─《 🚀 SPEEDTEST INFO 》
├ Upload: {result['upload']} MB/s
├ Download: {result['download']} MB/s
├ Ping: {result['ping']} ms
├ Time: {result['timestamp']}
├ Data Sent: {result['bytes_sent']} MB
╰ Data Received: {result['bytes_received']} MB

╭─《 🌐 SPEEDTEST SERVER 》
├ Name: {s['name']}
├ Country: {s['country']}
├ Sponsor: {s['sponsor']}
├ Latency: {result['ping']} ms
├ Latitude: {s['lat']}
╰ Longitude: {s['lon']}

╭─《 👤 CLIENT DETAILS 》
├ IP Address: {c['ip']}
├ Latitude: {c['lat']}
├ Longitude: {c['lon']}
├ Country: {c['country']}
├ ISP: {c['isp']}
├ ISP Rating: N/A
╰ Powered by कार्तिक
"""

    await bot.send_photo(
        chat_id=GROUP_ID,
        photo=result["image"],
        caption=caption
    )
    async def monitor():

    try:
        await bot.send_message(
            chat_id=GROUP_ID,
            text="🚀 Speedtest Bot Started Successfully!"
        )
    except Exception as e:
        logging.error(f"Startup message failed: {e}")

    while True:
        try:
            await send()
            logging.info("✅ Speedtest sent successfully")

        except Exception as e:
            logging.exception(f"Speedtest Error: {e}")

            try:
                await bot.send_message(
                    chat_id=GROUP_ID,
                    text=f"❌ Speedtest Failed!\n\nReason:\n{e}"
                )
            except Exception:
                pass

        await asyncio.sleep(INTERVAL)


async def main():
    logging.info("🚀 Bot Started")
    await monitor()


if __name__ == "__main__":
    asyncio.run(main())
