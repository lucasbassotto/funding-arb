import asyncio
import ccxt.async_support as ccxt
import redis
import json
import time
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from threading import Thread

# === CONFIGURAÇÕES ===
FEE = 0.001
ORDER_SIZE = 50  # USD
MAX_POSITION_USD = 50000  # Exposição máxima por exchange
DRY_RUN = False
REDIS_CHANNEL = "arbitrage_signals"
RETRY_LIMIT = 3
TICK_TIMEOUT = 10  # segundos

# === EXCHANGES ===
EXCHANGES = {
    "binance": ccxt.binance({
        "apiKey": "",
        "secret": "",
        "options": {"defaultType": "future"}
    }),
    "bybit": ccxt.bybit({
        "apiKey": "",
        "secret": "",
        "options": {"defaultType": "linear"}
    }),
    "okx": ccxt.okx({
        "apiKey": "",
        "secret": "",
        "password": "",
        "options": {"defaultType": "swap"}
    }),
}

PRECISION_CACHE = {}
LAST_TICK = {}
r = redis.Redis()
EXECUTION_ALLOWED = {"status": True}

EXCHANGE_ALIAS = {
    "binancefutures": "binance",
    "bybitfutures": "bybit",
    "okxfutures": "okx"
}

# === FUNÇÕES AUXILIARES ===

def get_precision(exchange_id, symbol):
    if exchange_id not in PRECISION_CACHE:
        precisions_raw = r.get(f"precisions:{exchange_id}")
        if precisions_raw:
            PRECISION_CACHE[exchange_id] = json.loads(precisions_raw)
        else:
            PRECISION_CACHE[exchange_id] = {}
    return PRECISION_CACHE[exchange_id].get(symbol, 6)

def normalize_symbol(exchange, symbol):
    if exchange == "bybit":
        return symbol.replace("/", "")
    elif exchange == "okx":
        return symbol.replace("/", "-") + "-SWAP"
    return symbol

async def get_position(exchange, _):
    try:
        positions = await exchange.fetch_positions()
        total = sum(abs(float(p.get('notional', 0.0))) for p in positions)
        return total
    except:
        return 0.0

async def place_order(exchange, symbol, side, amount, price):
    for _ in range(RETRY_LIMIT):
        try:
            precision = get_precision(exchange.id, symbol)
            amount = Decimal(amount).quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN)

            print(f"[DEBUG] {exchange.id.upper()} | {side.upper()} {symbol} | price: {price}, amount: {amount}")

            if DRY_RUN:
                print(f"[DRY RUN] {side.upper()} {symbol} @ {price:.4f} ({amount})")
                return {
                    "symbol": symbol,
                    "side": side,
                    "amount": float(amount),
                    "price": price,
                    "timestamp": time.time(),
                    "exchange": exchange.id,
                    "status": "dry-run",
                    "expected_price": price
                }

            order = await exchange.create_order(symbol, 'market', side, float(amount))
            order['expected_price'] = price
            return order

        except Exception as e:
            print(f"[ORDER ERROR] {exchange.id.upper()} {side.upper()} {symbol}: {e}")
            await asyncio.sleep(1)
    return None

async def execute_trade(signal):
    if not EXECUTION_ALLOWED["status"]:
        print("[BLOCKED] Execuções desabilitadas por watchdog.")
        return

    raw_symbol = signal['symbol']
    buy_ex = EXCHANGE_ALIAS.get(signal['buy_exchange'], signal['buy_exchange'])
    sell_ex = EXCHANGE_ALIAS.get(signal['sell_exchange'], signal['sell_exchange'])

    price_buy = signal['buy_price']
    price_sell = signal['sell_price']

    ex_buy = EXCHANGES[buy_ex]
    ex_sell = EXCHANGES[sell_ex]

    norm_symbol_buy = normalize_symbol(buy_ex, raw_symbol)
    norm_symbol_sell = normalize_symbol(sell_ex, raw_symbol)

    exposure_buy = await get_position(ex_buy, norm_symbol_buy)
    exposure_sell = await get_position(ex_sell, norm_symbol_sell)

    if exposure_buy > MAX_POSITION_USD or exposure_sell > MAX_POSITION_USD:
        print("[BLOCKED] Exposição excedida. Ignorando trade.")
        return

    price_base = max(price_buy, price_sell)
    amount = ORDER_SIZE / price_base

    print(f"[EXEC] {raw_symbol} | BUY on {buy_ex} / SELL on {sell_ex}")
    ts_received = datetime.utcnow().isoformat()

    buy_task = place_order(ex_buy, norm_symbol_buy, 'buy', amount, price_buy)
    sell_task = place_order(ex_sell, norm_symbol_sell, 'sell', amount, price_sell)
    results = await asyncio.gather(buy_task, sell_task)

    ts_sent = datetime.utcnow().isoformat()
    trade_log = {
        "symbol": raw_symbol,
        "buy_exchange": buy_ex,
        "sell_exchange": sell_ex,
        "buy_order": results[0],
        "sell_order": results[1],
        "buy_expected_price": price_buy,
        "sell_expected_price": price_sell,
        "timestamp_received": ts_received,
        "timestamp_sent": ts_sent
    }

async def redis_listener():
    pubsub = r.pubsub()
    pubsub.subscribe(REDIS_CHANNEL)
    print("[READY] Aguardando sinais...")

    while True:
        message = pubsub.get_message()
        if message and message['type'] == 'message':
            try:
                signal = json.loads(message['data'])
                await execute_trade(signal)
            except Exception as e:
                print("Erro ao processar sinal:", e)
        await asyncio.sleep(0.01)

def watchdog():
    while True:
        now = time.time()
        any_down = False
        for exchange, ts in LAST_TICK.items():
            if now - ts > TICK_TIMEOUT:
                print(f"[WATCHDOG] {exchange} sem ticks por mais de {TICK_TIMEOUT}s! Bloqueando execução.")
                any_down = True
        EXECUTION_ALLOWED["status"] = not any_down
        time.sleep(5)

# === INÍCIO ===
if __name__ == "__main__":
    Thread(target=watchdog, daemon=True).start()
    asyncio.run(redis_listener())
