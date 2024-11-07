import json
from websocket import create_connection

def test_websocket_connection(type):
    market_codes = ["KRW-BTC"]

    try:
        ws = create_connection("wss://api.upbit.com/websocket/v1")
        ws.send(json.dumps([
            {"ticket": "test"},
            {"type": type, "codes": market_codes, "format": "SIMPLE"}
        ]))

        while True:
            result = ws.recv()
            print("Received data: ", result)

    except Exception as e:
        print(f"Error in webscoket connection {e}")

if __name__ == "__main__":
    type_input = input("Enter the type(ticker, trade, orderbook): ")
    test_websocket_connection(type_input)