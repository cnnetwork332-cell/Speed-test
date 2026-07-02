import os
import asyncio
import speedtest
from telegram import Bot

BOT_TOKEN = os.environ.get("BOT_TOKEN")

# Group ID (example: -1001234567890)
GROUP_ID = int(os.environ.get("GROUP_ID"))

bot = Bot(token=BOT_TOKEN)


def run_speedtest():
    try:
        st = speedtest.Speedtest()

        st.get_best_server()
        st.download()
        st.upload()

        result = st.results.share()

        return result

    except Exception as e:
        print("Speedtest error:", e)
        return None


async def monitor():
    while True:
        try:
            link = run_speedtest()

            if link:
                await bot.send_message(
                    chat_id=GROUP_ID,
                    text=f"📶 Speedtest Result\n{link}",
                    disable_web_page_preview=False
                )
                print("Sent:", link)

        except Exception as e:
            print("Telegram error:", e)

        await asyncio.sleep(13)


async def main():
    print("Speedtest monitor started")
    await monitor()


if name == "main":
    asyncio.run(main())
