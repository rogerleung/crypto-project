from websockets.sync.client import connect
import asyncio
import websocket
import zmq

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind('tcp://127.0.0.1:5555')

with connect("wss://stream.binance.com:9443/ws") as websocket:
    #Subscription
    json_subscribe = '{"method": "SUBSCRIBE", "params": ["shibusdt@kline_1s","maticusdt@kline_1s","solusdt@kline_1s","btcusdt@kline_1s","bnbusdt@kline_1s","dogeusdt@kline_1s"],  "id": 2}' #other pairs
    websocket.send(json_subscribe)
    while True:
        message = "CRYPTO " + websocket.recv()
        socket.send_string(message)
        print(f"Received: {message}")