import asyncio
import ccxt.async_support as ccxt
import redis
import json

EXCHANGES = {
    "binance": ccxt.binance({"options": {"defaultType": "future"}}),
    "bybit": ccxt.bybit({"options": {"defaultType": "linear"}}),
    "okx": ccxt.okx({"options": {"defaultType": "swap"}}),
}

r = redis.Redis()

async def fetch_precisions(exchange_id, exchange):
    try:
        await exchange.load_markets()
        precisions = {
            symbol: market["precision"]["amount"]
            for symbol, market in exchange.markets.items()
            if market.get("precision", {}).get("amount") is not None
        }
        r.set(f"precisions:{exchange_id}", json.dumps(precisions))
        print(f"[✓] {exchange_id} precisions atualizados ({len(precisions)})")
    except Exception as e:
        print(f"[X] Falha ao buscar precisions para {exchange_id}: {e}")
    finally:
        await exchange.close()

async def main():
    tasks = [fetch_precisions(eid, ex) for eid, ex in EXCHANGES.items()]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
