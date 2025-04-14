import redis
import json
import time
import os

FEE = 0.001
FUNDING_WEIGHT = 1
MIN_SPREAD = 0.0
MIN_CARRY = 0.1
MAX_COUNTDOWN_MINUTES = 60

redis_client = redis.Redis()
REDIS_CHANNEL = "arbitrage_signals"
PUBSUB_CHANNEL = "cbbo_ticks"

def normalize_symbol(symbol: str) -> str:
    return symbol.replace("-USDT.PERP", "/USDT") \
                 .replace("-USDT", "/USDT") \
                 .replace("-PERP", "") \
                 .replace(".PERP", "") \
                 .replace(".perp", "") \
                 .upper() \
                 .replace("-USD.PERP", "/USDT")

def process_snapshot(snapshot):
    symbol = snapshot["symbol"]
    bids = snapshot.get("bids", {})
    asks = snapshot.get("asks", {})
    funding_data = snapshot.get("funding", {})
    funding_times = snapshot.get("funding_time", {})
    funding_countdowns = snapshot.get("funding_countdown", {})

    for buy_exch, ask_data in asks.items():
        for sell_exch, bid_data in bids.items():
            if buy_exch == sell_exch:
                continue

            t1 = funding_times.get(buy_exch)
            t2 = funding_times.get(sell_exch)
            if not t1 or not t2 or t1 != t2:
                continue

            # Countdown em minutos
            countdown = funding_countdowns.get(buy_exch)
            if countdown is None:
                continue
            if countdown > MAX_COUNTDOWN_MINUTES:
                continue

            buy_price = ask_data["price"] * (1 + FEE)
            sell_price = bid_data["price"] * (1 - FEE)

            if sell_price <= buy_price:
                continue

            spread = sell_price - buy_price
            spread_percent = (spread / buy_price) * 100
            size = min(ask_data["qty"], bid_data["qty"])

            buy_funding = funding_data.get(buy_exch, 0) * 100
            sell_funding = funding_data.get(sell_exch, 0) * 100

            if buy_funding < 0:
                funding_carry = (-1 * buy_funding) + sell_funding
            else:
                funding_carry = sell_funding - buy_funding

            net_spread_percent = spread_percent + FUNDING_WEIGHT * funding_carry

            if net_spread_percent > MIN_SPREAD and funding_carry > MIN_CARRY:
                opportunity = {
                    'symbol': symbol,
                    'buy_exchange': buy_exch,
                    'buy_price': ask_data['price'],
                    'sell_exchange': sell_exch,
                    'sell_price': bid_data['price'],
                    'spread_percent': round(net_spread_percent, 6),
                    'raw_spread_percent': round(spread_percent, 6),
                    'buy_funding': round(buy_funding, 6),
                    'sell_funding': round(sell_funding, 6),
                    'funding_countdown': countdown,
                    'size': size
                }

                redis_client.publish(REDIS_CHANNEL, json.dumps(opportunity))
                opps_file = "opps.json"
                existing_opps = []

                if os.path.exists(opps_file):
                    try:
                        with open(opps_file, "r") as f:
                            existing_opps = json.load(f)
                            if not isinstance(existing_opps, list):
                                existing_opps = []
                    except Exception:
                        existing_opps = []

                existing_opps.append(opportunity)

                with open(opps_file, "w") as f:
                    json.dump(existing_opps, f, indent=2)

def main():
    pubsub = redis_client.pubsub()
    pubsub.subscribe(PUBSUB_CHANNEL)
    print(f"🔌 Aguardando ticks no canal '{PUBSUB_CHANNEL}'...\n")

    for message in pubsub.listen():
        if message["type"] != "message":
            continue
        try:
            snapshot = json.loads(message["data"])
            process_snapshot(snapshot)
        except Exception as e:
            print(f"[ERRO] Falha ao processar tick: {e}")

if __name__ == "__main__":
    main()
