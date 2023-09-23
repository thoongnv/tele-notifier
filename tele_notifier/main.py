import asyncio
import os

import httpx
import redis
import redis.asyncio as asyncredis
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://:secretkey@localhost:6379/0")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "bot_token")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "channel_id")


async def main():
    client = httpx.AsyncClient()
    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    conn = asyncredis.Redis.from_url(REDIS_URL)
    errors = 0

    while True:
        try:
            msg = await conn.lpop("notify.queue")
            if msg:
                params = {
                    "text": msg.decode("utf-8"),
                    "chat_id": TELEGRAM_CHANNEL_ID,
                }
                resp = await client.get(api_url, params=params)
                print(resp)

                if resp.status_code == 429:
                    await asyncio.sleep(int(resp.headers.get("Retry-After", 0)) + 10)

                    # Re-send message
                    resp = await client.get(api_url, params=params)
                    print(resp)
        except Exception as e:
            print(f"Error: {e}")

            if isinstance(e, redis.exceptions.ConnectionError):
                errors += 1
                if errors > 100:
                    # Reset connection
                    conn = asyncredis.Redis()
                    errors = 0


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
