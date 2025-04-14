import requests
import json
import time
from collections import defaultdict
import redis

r = redis.Redis()

headers = {
    "accept": "application/json",
    "CG-API-KEY": "1ff68c89ae8b4335b65b817f00dc04d1"
}

def get_funding_data_from_coinglass():
    url = "https://open-api-v3.coinglass.com/api/futures/fundingRate/exchange-list"
    response = requests.get(url, headers=headers, timeout=10)
    raw_data = response.json().get("data", [])

    funding_data = defaultdict(dict)

    for item in raw_data:
        symbol = item.get("symbol")
        entries = item.get("usdtOrUsdMarginList", [])
        for e in entries:
            exchange = e.get("exchange", "").lower()
            rate = e.get("fundingRate", None)

            if exchange in ["binance", "bybit", "okx", "gateio"] and rate is not None:
                normalized_symbol = symbol + "/USDT"
                funding_data[normalized_symbol][exchange] = float(rate)

    return funding_data
def get_top_funding_symbols(funding_data, top_n=25):
    spreads = []
    for symbol, exchanges in funding_data.items():
        rates = list(exchanges.values())
        if len(rates) >= 2:
            spread = abs(max(rates) - min(rates))
            spreads.append((symbol, spread))
    spreads.sort(key=lambda x: x[1], reverse=True)
    top_symbols = [symbol.replace("/", "-") + ".PERP" for symbol, _ in spreads[:top_n]]
    return top_symbols

def update_funding_data():
    print("\n[INFO] Atualizando funding rates via CoinGlass...")
    
    funding_data = get_funding_data_from_coinglass()

    with open("funding_cache.json", "w") as f:
        json.dump(funding_data, f, indent=2)

    top_symbols = get_top_funding_symbols(funding_data)
    r.set("funding_top_symbols", json.dumps(top_symbols))

    print(f"[✓] Top {len(top_symbols)} símbolos salvos em funding_top_symbols.\n")

if __name__ == "__main__":
    while True:
        update_funding_data()
        print("[INFO] Aguardando 5 minutos para próxima execução...\n")
        time.sleep(600)