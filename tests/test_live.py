import asyncio
import json
from unittest.mock import AsyncMock, patch

import pytest

from ws.live import LiveTickerStream

def make_kline_msg(open_ts, close_ts, open_p="10000", high_p="10010", low_p="9990", close_p="10005", vol="0.1", closed=True):
    return {
        "e": "kline",
        "E": int(open_ts + 1000),
        "s": "BTCUSDT",
        "k": {
            "t": int(open_ts),
            "T": int(close_ts),
            "s": "BTCUSDT",
            "i": "1m",
            "f": 0,
            "L": 0,
            "o": str(open_p),
            "c": str(close_p),
            "h": str(high_p),
            "l": str(low_p),
            "v": str(vol),
            "n": 0,
            "x": bool(closed),
            "q": "0",
            "V": "0",
            "B": "0",
        },
    }

@pytest.mark.asyncio
async def test_handle_message_parses_and_appends():
    stream = LiveTickerStream("BTCUSDT", "1m", buffer_size=10)
    now = int((await asyncio.get_event_loop().time()) * 1000)
    msg = make_kline_msg(open_ts=now, close_ts=now + 60000, closed=False)
    await stream._handle_message(msg)
    latest = stream.get_latest()
    assert latest is not None
    assert latest["open"] == float(msg["k"]["o"])  # parsed float
    assert latest["closed"] is False
    assert len(stream.get_all()) == 1

@pytest.mark.asyncio
async def test_closed_candle_detection():
    stream = LiveTickerStream("BTCUSDT", "1m", buffer_size=10)
    now = int((await asyncio.get_event_loop().time()) * 1000)
    msg = make_kline_msg(open_ts=now, close_ts=now + 60000, closed=True)
    await stream._handle_message(msg)
    assert stream.last_closed_candle is not None
    assert stream.last_closed_candle["closed"] is True

@pytest.mark.asyncio
async def test_ring_buffer_max_size():
    stream = LiveTickerStream("BTCUSDT", "1m", buffer_size=2)
    now = int((await asyncio.get_event_loop().time()) * 1000)
    for i in range(3):
        msg = make_kline_msg(open_ts=now + i * 60000, close_ts=now + (i + 1) * 60000, closed=True)
        await stream._handle_message(msg)
    allc = stream.get_all()
    assert len(allc) == 2

@pytest.mark.asyncio
async def test_connect_success_and_receive(monkeypatch):
    # prepare fake websocket with recv returning one message then cancelling
    now = int((await asyncio.get_event_loop().time()) * 1000)
    msg = make_kline_msg(open_ts=now, close_ts=now + 60000, closed=True)
    ws = AsyncMock()
    ws.recv = AsyncMock(side_effect=[json.dumps(msg), asyncio.CancelledError()])
    ws.close = AsyncMock()

    async def fake_connect(url, **kwargs):
        return ws

    monkeypatch.setattr('websockets.connect', fake_connect)

    stream = LiveTickerStream("BTCUSDT", "1m", buffer_size=10)
    await stream.connect()
    # give some time for listener to process
    await asyncio.sleep(0.05)
    # Stop stream
    await stream.stop()
    # buffer should contain parsed message
    latest = stream.get_latest()
    assert latest is not None
    assert latest['closed'] is True

@pytest.mark.asyncio
async def test_reconnect_on_exception(monkeypatch):
    # First connect attempt raises, second attempt returns ws
    now = int((await asyncio.get_event_loop().time()) * 1000)
    msg = make_kline_msg(open_ts=now, close_ts=now + 60000, closed=True)
    ws = AsyncMock()
    ws.recv = AsyncMock(side_effect=[json.dumps(msg), asyncio.CancelledError()])
    ws.close = AsyncMock()

    async def fake_connect_fail(url, **kwargs):
        raise ConnectionError("fail")

    async def fake_connect_ok(url, **kwargs):
        return ws

    calls = {'n': 0}

    async def fake_connect_switch(url, **kwargs):
        calls['n'] += 1
        if calls['n'] == 1:
            return await fake_connect_fail(url, **kwargs)
        return await fake_connect_ok(url, **kwargs)

    monkeypatch.setattr('websockets.connect', fake_connect_switch)

    stream = LiveTickerStream("BTCUSDT", "1m", buffer_size=10)
    # connect should eventually succeed (after handling first failure)
    await stream.connect()
    await asyncio.sleep(0.05)
    await stream.stop()
    latest = stream.get_latest()
    assert latest is not None
