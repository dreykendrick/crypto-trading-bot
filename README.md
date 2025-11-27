## Live Streaming (ws/live.py)

Provides an async WebSocket-based kline/ticker stream for Binance.

Features:
- Asyncio-based websocket client using `websockets`
- Auto-reconnect with exponential backoff
- In-memory ring buffer storing last N candles
- Distinguishes open vs closed candles

Example:

```python
from ws.live import LiveTickerStream
import asyncio

async def demo():
    s = LiveTickerStream("BTCUSDT", "1m")
    await s.connect()
    await asyncio.sleep(2)
    print(s.get_latest())
    await s.stop()

asyncio.run(demo())
```

Run tests:

pytest tests/test_live.py -q
