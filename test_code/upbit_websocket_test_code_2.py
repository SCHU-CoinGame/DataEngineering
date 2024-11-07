import asyncio
import websockets
import json


async def test_websocket_connection():
    market_codes = ["KRW-BTC"]
    uri = "wss://api.upbit.com/websocket/v1"

    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps([
            {"ticket": "test"},
            {"type": "ticker", "codes": ["KRW-BTC"], "format": "SIMPLE"}
        ]))

        while True:
            result = await ws.recv()
            print("Received data: ", result)


if __name__ == "__main__":
    asyncio.run(test_websocket_connection())
