import asyncio
import json
import websockets
import redis
import time
from datetime import datetime

TOKEN = "da94acb15ff6c7f6f18ee32890611c84dd6c606e"
URL = "wss://trading-api.coinroutes.io/api/streaming/cbbo/"

redis_client = redis.Redis(host="localhost", decode_responses=True)
PUBSUB_CHANNEL = "cbbo_ticks"

EXCHANGES = ["binancefutures", "bybit"]
SUBSCRIPTION_REFRESH_INTERVAL = 600  # 10 minutos

def get_symbols_from_redis(limit=25):
    raw = redis_client.get("funding_top_symbols")
    if raw:
        try:
            symbols = json.loads(raw)
            return symbols[:limit]
        except json.JSONDecodeError:
            print("[ERRO] Não foi possível decodificar funding_top_symbols do Redis.")
    print("[AVISO] Nenhum símbolo encontrado no Redis. Usando fallback.")
    return []

def normalize_symbol(symbol):
    base = symbol.replace(".PERP", "").replace("-PERP", "").replace("-USDT", "")
    if base in ["BTC", "ETH"]:
        return f"{base}-USD.PERP"  # Hyperliquid
    return f"{base}-USDT.PERP"

async def subscribe(ws, symbols):
    for symbol in symbols:
        norm_symbol = normalize_symbol(symbol)
        msg = {
            "currency_pair": norm_symbol,
            "size_filter": 0.0,
            "sample": 0.1,
            "subscribe": True,
            "exchanges": EXCHANGES
        }
        await ws.send(json.dumps(msg))
        print(f"[SUBSCRIBED] {norm_symbol}")
        await asyncio.sleep(0.05)

async def handle_cbbo(data):
    symbol = data.get("product")
    timestamp = data.get("generated_timestamp")

    bids = {}
    asks = {}

    for bid in data.get("bids", []):
        exch = bid["exchange"].lower()
        price = float(bid["price"])
        qty = float(bid["qty"])
        if exch not in bids or price > bids[exch]["price"]:
            bids[exch] = {"price": price, "qty": qty}

    for ask in data.get("asks", []):
        exch = ask["exchange"].lower()
        price = float(ask["price"])
        qty = float(ask["qty"])
        if exch not in asks or price < asks[exch]["price"]:
            asks[exch] = {"price": price, "qty": qty}

    funding_raw = data.get("properties", {}).get("funding_rate", {})
    funding_rates = {
        exch.lower(): float(rate)
        for exch, rate in funding_raw.items()
    }

    funding_times_raw = data.get("properties", {}).get("next_funding_time", {})
    funding_times = {}
    funding_countdowns = {}

    now = int(time.time())

    for exch, ts in funding_times_raw.items():
        try:
            ts_int = int(ts)
            if ts_int > 1e11:  # Bybit vem em ms
                ts_int = ts_int // 1000
            exch_l = exch.lower()
            funding_times[exch_l] = ts_int
            funding_countdowns[exch_l] = int((ts_int - now) / 60)  # em minutos
        except:
            continue

    snapshot = {
        "symbol": symbol.replace("-USDT.PERP", "/USDT").replace("-USD.PERP", "/USDT"),
        "timestamp": timestamp,
        "bids": bids,
        "asks": asks,
        "funding": funding_rates,
        "funding_time": funding_times,
        "funding_countdown": funding_countdowns
    }

    redis_key = f"cbbo:{snapshot['symbol']}"
    redis_client.setex(redis_key, 60, json.dumps(snapshot))
    redis_client.publish(PUBSUB_CHANNEL, json.dumps(snapshot))
    #print(f"[CBBO] {snapshot['symbol']} | bids: {len(bids)} | asks: {len(asks)} | funding: {funding_rates} | countdown: {funding_countdowns} | funding_time: {funding_times}")

async def main():
    headers = {"Authorization": f"Token {TOKEN}"}
    async with websockets.connect(
        URL,
        extra_headers=headers,
        ping_interval=20,
        ping_timeout=10,
        max_queue=None
    ) as ws:
        while True:
            symbols = get_symbols_from_redis()
            await subscribe(ws, symbols)
            start_time = asyncio.get_event_loop().time()

            try:
                while True:
                    message = await asyncio.wait_for(ws.recv(), timeout=5)
                    data = json.loads(message)
                    if "bids" in data and "asks" in data:
                        await handle_cbbo(data)

                    if asyncio.get_event_loop().time() - start_time > SUBSCRIPTION_REFRESH_INTERVAL:
                        print("[REFRESH] Renovando subscrição dos símbolos...")
                        break

            except Exception as e:
                print(f"[ERRO] {e}")

async def connect_loop():
    while True:
        try:
            await main()
        except Exception as e:
            print(f"[RECONNECT] WebSocket desconectado: {e}. Reconectando em 5s...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(connect_loop())
