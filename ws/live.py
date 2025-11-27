"""
Async Live WebSocket streamer for Binance klines.
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional

import websockets
import pandas as pd

from data.historical import incremental_update, load_cached  # imported for reference/sync

logger = logging.getLogger(__name__)


class LiveTickerStream:
    """Connects to Binance kline websocket and maintains an in-memory ring buffer of recent candles.

    Features:
    - auto-reconnect with exponential backoff
    - parses kline messages and stores normalized dicts in a deque
    - exposes synchronous getters for latest/all candles
    """

    def __init__(self, symbol: str, interval: str = "1m", buffer_size: int = 1000):
        self.symbol = symbol.upper()
        self.interval = interval
        self.buffer_size = int(buffer_size)
        self.buffer: Deque[Dict[str, Any]] = deque(maxlen=self.buffer_size)
        self.last_closed_candle: Optional[Dict[str, Any]] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._listener_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._connected_event = asyncio.Event()
        self._lock = asyncio.Lock()

        self.ws_url = f"wss://stream.binance.com:9443/ws/{self.symbol.lower()}@kline_{self.interval}"

    async def connect(self):
        """Open websocket connection and start listener task."""
        logger.info("Connecting to %s", self.ws_url)
        self._stop_event.clear()
        await self._reconnect()

    async def _listen(self):
        """Continuously listen for messages and handle them."""
        assert self._ws is not None
        try:
            while not self._stop_event.is_set():
                try:
                    msg = await self._ws.recv()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("Websocket recv error: %s", exc)
                    # trigger reconnect
                    await self._reconnect()
                    return
                # parse message
                try:
                    data = json.loads(msg) if isinstance(msg, (str, bytes)) else msg
                except Exception:
                    logger.debug("Invalid JSON received, skipping")
                    continue
                await self._handle_message(data)
        except asyncio.CancelledError:
            logger.info("Listener task cancelled")
        finally:
            logger.info("Listener exiting")

    async def _handle_message(self, msg: Dict[str, Any]):
        """Process a parsed websocket message from Binance kline stream."""
        if not isinstance(msg, dict):
            return
        k = msg.get("k")
        if not k:
            return
        try:
            open_time = int(k.get("t"))
            close_time = int(k.get("T"))
            candle = {
                "timestamp": pd.to_datetime(open_time, unit='ms', utc=True),
                "open": float(k.get("o")),
                "high": float(k.get("h")),
                "low": float(k.get("l")),
                "close": float(k.get("c")),
                "volume": float(k.get("v")),
                "close_time": pd.to_datetime(close_time, unit='ms', utc=True),
                "closed": bool(k.get("x", False)),
            }
        except Exception as e:
            logger.debug("Failed to normalize kline message: %s", e)
            return
        # Append to buffer (thread-safe in asyncio single-threaded loop)
        self.buffer.append(candle)
        if candle.get("closed"):
            self.last_closed_candle = candle
        logger.debug("Appended candle ts=%s closed=%s", candle["timestamp"], candle["closed"]) 

    async def _reconnect(self):
        """Try to establish websocket connection with exponential backoff."""
        backoff = 1.0
        max_backoff = 30.0
        # Cancel existing listener if any
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            try:
                await self._listener_task
            except Exception:
                pass
        while not self._stop_event.is_set():
            try:
                logger.info("Attempting websocket connect to %s", self.ws_url)
                self._ws = await websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20)
                logger.info("Websocket connected to %s", self.ws_url)
                self._connected_event.set()
                # start listener
                self._listener_task = asyncio.create_task(self._listen())
                return
            except Exception as e:
                logger.warning("Websocket connect failed: %s. Backing off %.1fs", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    def get_latest(self) -> Optional[Dict[str, Any]]:
        """Return newest candle or None"""
        try:
            return self.buffer[-1] if self.buffer else None
        except Exception:
            return None

    def get_all(self) -> List[Dict[str, Any]]:
        """Return snapshot list of buffer contents"""
        return list(self.buffer)

    async def stop(self):
        """Stop the listener and close websocket cleanly."""
        logger.info("Stopping LiveTickerStream for %s", self.symbol)
        self._stop_event.set()
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            try:
                await self._listener_task
            except Exception:
                pass
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        self._connected_event.clear()
        logger.info("Stopped LiveTickerStream for %s", self.symbol)
