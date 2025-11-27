import asyncio
from ws.live import LiveTickerStream

async def main():
    stream = LiveTickerStream("BTCUSDT", "1m")
    await stream.connect()
    try:
        for _ in range(5):
            await asyncio.sleep(1)
            print(stream.get_latest())
    finally:
        await stream.stop()

if __name__ == "__main__":
    asyncio.run(main())
