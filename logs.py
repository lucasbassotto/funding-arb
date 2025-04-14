import time
import hmac
import hashlib
import requests
import pandas as pd
from pybit.unified_trading import HTTP
from collections import defaultdict
import sqlite3
import datetime
import sys
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import ccxt
ccxt_client = ccxt.bybit()

# ========= CONFIGURAÇÕES =========
BINANCE_API_KEY = ""
BINANCE_API_SECRET = ""
BYBIT_API_KEY = ""
BYBIT_API_SECRET = ""
GATE_API_KEY = ""
GATE_API_SECRET = ""
HYPER_USER = ""
COINROUTES_TOKEN = ""

EXCHANGE_MAP = {
    "binance": "binancefutures",
    "bybit": "bybit",
    "gateio": "gateio",
    "hyperliquid": "hyperliquid"
}

def extract_coin(symbol):
    for suffix in ["USDT", "USD", "-USDT", "-USD", "/USDT", "/USD", ".PERP"]:
        symbol = symbol.replace(suffix, "")
    return symbol.upper()

# ========= BINANCE =========
def sign_binance(params, secret):
    query_string = "&".join([f"{k}={v}" for k, v in params.items()])
    return hmac.new(secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()

def fetch_binance_income(start_ts, end_ts):
    url = "https://fapi.binance.com/fapi/v1/income"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    limit = 1000
    params = {
        "limit": limit,
        "startTime": start_ts,
        "endTime": end_ts,
        "timestamp": int(time.time() * 1000),
    }
    params["signature"] = sign_binance(params, BINANCE_API_SECRET)
    data = []
    while True:
        r = requests.get(url, headers=headers, params=params)
        res = r.json()
        if not res or not isinstance(res, list): break
        data.extend(res)
        if len(res) < limit: break
        params["startTime"] = res[-1]["time"] + 1
        params["timestamp"] = int(time.time() * 1000)
        params["signature"] = sign_binance(params, BINANCE_API_SECRET)
        time.sleep(0.1)
    return data

def parse_binance(data):
    out = defaultdict(lambda: {"exchange": "binance", "symbol": "", "funding": 0.0, "fees": 0.0, "realized_pnl": 0.0, "unrealized_pnl": 0.0, "position_size": 0.0})
    for i in data:
        s = i["symbol"]
        out[s]["symbol"] = s
        if i["incomeType"] == "FUNDING_FEE": out[s]["funding"] += float(i["income"])
        elif i["incomeType"] == "COMMISSION": out[s]["fees"] -= abs(float(i["income"]))
        elif i["incomeType"] == "REALIZED_PNL": out[s]["realized_pnl"] += float(i["income"])
    return list(out.values())

# ========= BYBIT =========
def fetch_bybit_logs(start_ts, end_ts):
    session = HTTP(api_key=BYBIT_API_KEY, api_secret=BYBIT_API_SECRET, testnet=False)
    cursor = None
    all_data = []
    while True:
        r = session.get_transaction_log(accountType="UNIFIED", startTime=start_ts, endTime=end_ts, limit=200, cursor=cursor)
        res = r.get("result", {}).get("list", [])
        if not res: break
        all_data.extend(res)
        cursor = r.get("result", {}).get("nextPageCursor")
        if not cursor: break
    return all_data

def parse_bybit(data):
    bybit = ccxt.bybit()
    out = defaultdict(lambda: {
        "exchange": "bybit",
        "symbol": "",
        "funding": 0.0,
        "fees": 0.0,
        "realized_pnl": 0.0,
        "unrealized_pnl": 0.0,
        "position_size": 0.0,
        "borrow_cost": 0.0,
        "borrow_pnl": 0.0
    })

    borrow_spot_tracker = defaultdict(lambda: {"coin_balance": 0.0, "usdt_balance": 0.0})

    for i in data:
        symbol = i.get("symbol", "")
        currency = i.get("currency", "")
        category = i.get("category", "").lower()
        ttype = i.get("type", "").upper()
        side = i.get("side", "").lower()

        fee = float(i.get("fee", 0))
        funding = float(i.get("funding", 0))
        cash_flow = float(i.get("cashFlow", 0))
        qty = float(i.get("qty", 0))

        key = symbol if symbol else currency
        out[key]["symbol"] = key

        if ttype == "FUNDING":
            out[key]["funding"] += funding
        elif ttype == "TRADE" and category=="linear":
            out[key]["realized_pnl"] += cash_flow
            out[key]["fees"] -= abs(fee)
        elif ttype == "TRADE":
            out[key]["fees"] -= abs(fee)
        elif ttype == "SETTLEMENT":
            out[key]["funding"] += funding
        elif ttype == "INTEREST":
            out[currency]["symbol"] = currency
            # Pega o ticker
            ticker = bybit.fetch_ticker(f"{currency}/USDT")
            # Extrai o último preço (last price)
            last_price = ticker['last']
            out[currency]["borrow_cost"] += abs(cash_flow) * last_price

        # Track spot-only trades for borrow_pnl
        if category == "spot" and ttype == "TRADE":
            if currency == "USDT":
                borrow_spot_tracker[symbol]["usdt_balance"] += cash_flow
            else:
                borrow_spot_tracker[symbol]["coin_balance"] += qty

    # Calcula o borrow_pnl final com preço de mercado
    for coin, stats in borrow_spot_tracker.items():
        coin_balance = stats["coin_balance"]
        usdt_balance = stats["usdt_balance"]

        try:
            ticker = bybit.fetch_ticker(f"{coin}")
            price = float(ticker["last"])
        except Exception as e:
            print(f"[ERROR] Falha ao buscar preço de {coin}/USDT: {e}")
            continue

        if coin_balance < 0:
            recomprar = abs(coin_balance) * price
            pnl = usdt_balance - recomprar
        elif coin_balance > 0:
            revender = coin_balance * price
            pnl = revender - usdt_balance
        else:
            pnl = usdt_balance

        # Garante que a chave do out para o borrow_pnl existe com símbolo já preenchido
        if coin not in out or not out[coin]["symbol"]:
            out[coin]["symbol"] = coin
            out[coin]["exchange"] = "bybit"

        out[coin]["borrow_pnl"] = pnl

    return list(out.values())
# ========= GATE.IO =========
def sign_gateio(method, url_path, query_string, body, api_key, api_secret):
    t = str(int(time.time()))
    hashed = hashlib.sha512((body or "").encode()).hexdigest()
    msg = "\n".join([method.upper(), url_path, query_string, hashed, t])
    sig = hmac.new(api_secret.encode(), msg.encode(), hashlib.sha512).hexdigest()
    return {'KEY': api_key, 'Timestamp': t, 'SIGN': sig, 'Content-Type': 'application/json'}

def fetch_gateio_income(start_ts, end_ts):
    import requests
    import pandas as pd

    host = "https://api.gateio.ws"
    path = "/api/v4/futures/usdt/account_book/"
    url = host + path
    income = []
    page = 1

    while True:
        query = f"limit=100&page={page}&start_time={int(start_ts)}&end_time={int(end_ts - 1)}"
        headers = sign_gateio("GET", path, query, "", GATE_API_KEY, GATE_API_SECRET)
        r = requests.get(f"{url}?{query}", headers=headers)
        if r.status_code != 200:
            break
        res = r.json()
        if not res:
            break
        income.extend(res)
        if len(res) < 100:
            break
        page += 1

    # Remover duplicatas com base nas colunas relevantes
    df = pd.DataFrame(income)
    if df.empty:
        return []

    # Converte para datetime para filtro
    df["timestamp"] = pd.to_datetime(df["time"], unit="s")

    # Aplica o filtro exato pelo intervalo desejado
    df = df[(df["timestamp"] >= pd.to_datetime(start_ts, unit="s")) & 
            (df["timestamp"] <= pd.to_datetime(end_ts, unit="s"))]

    # Dedupe de verdade: id deve ser único
    df_clean = df.drop_duplicates(subset=["id"])
    return df_clean.to_dict(orient="records")

def parse_gateio(data):
    out = defaultdict(lambda: {"exchange": "gateio", "symbol": "", "funding": 0.0, "fees": 0.0, "realized_pnl": 0.0, "unrealized_pnl": 0.0, "position_size": 0.0})
    for i in data:
        symbol = i.get("contract", "").replace("_", "").upper()
        if symbol in ["", "-"]: continue
        out[symbol]["symbol"] = symbol
        t = i.get("type", "").lower()
        val = float(i.get("change", 0))
        if t == "fund": out[symbol]["funding"] += val
        elif t == "fee": out[symbol]["fees"] -= abs(val)
        elif "pnl" in i.get("text", "") or "profit" in i.get("text", ""): out[symbol]["realized_pnl"] += val
    return list(out.values())

# ========= HYPERLIQUID =========
def fetch_hyperliquid_funding(addr, start_ts, end_ts):
    r = requests.post("https://api.hyperliquid.xyz/info", json={"type": "userFunding", "user": HYPER_USER})
    return [i for i in r.json() if start_ts <= i["time"] <= end_ts]

def fetch_hyperliquid_fills(user_address, start_ts, end_ts):
    url = "https://api.hyperliquid.xyz/info"
    payload = {
        "type": "userFills",
        "user": HYPER_USER
    }
    try:
        res = requests.post(url, json=payload)
        data = res.json()
        return [fill for fill in data if start_ts <= fill["time"] <= end_ts]
    except Exception as e:
        print("[HYPERLIQUID FILLS] EXCEPTION:", e)
        return []

def parse_hyper(funding_data, fills_data):
    grouped = defaultdict(lambda: {
        "exchange": "hyperliquid",
        "symbol": "",
        "funding": 0.0,
        "fees": 0.0,
        "realized_pnl": 0.0,
        "unrealized_pnl": 0.0,
        "position_size": 0.0
    })

    # FUNDING
    for item in funding_data:
        coin = item.get("delta", {}).get("coin", "").upper()
        usdc = float(item.get("delta", {}).get("usdc", 0.0))
        symbol = f"{coin}"
        grouped[symbol]["symbol"] = symbol
        grouped[symbol]["funding"] += usdc  # já vem com sinal correto

    # FILLS
    for item in fills_data:
        coin = item.get("coin", "").upper()
        symbol = f"{coin}"
        grouped[symbol]["symbol"] = symbol

        fee = float(item.get("fee", 0.0))
        closed_pnl = float(item.get("closedPnl", 0.0))

        grouped[symbol]["fees"] -= abs(fee)  # fee pode ser positivo (rebate)
        grouped[symbol]["realized_pnl"] += closed_pnl

    return list(grouped.values())

# ========= POSIÇÕES COINROUTES =========
def fetch_coinroutes_positions():
    url = "https://trading-api.coinroutes.io/api/positions/"
    headers = {
        "Authorization": f"Token {COINROUTES_TOKEN}",
        "Accept": "application/json"
    }

    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print("[COINROUTES] ERRO:", response.text)
            return {}

        data = response.json()
        result = {}

        for item in data:
            raw_symbol = item.get("currency_pair", "")
            exchange = item.get("exchange", "").lower()
            if not raw_symbol or not exchange:
                continue

            symbol = raw_symbol.replace("-", "").replace(".PERP", "").replace("/", "").upper()
            coin_only = symbol.replace("USDT", "").replace("USD", "").upper()

            key = (coin_only, exchange)
            result[key] = {
                "unrealized_pnl": float(item.get("unrealized_pnl", 0.0)),
                "position_size": float(item.get("notional_value", 0.0))
            }

        return result

    except Exception as e:
        print("[COINROUTES] EXCEPTION:", e)
        return {}

def main():
    start_arg = sys.argv[1] if len(sys.argv) > 1 else "2001-04-07 00:00:00"
    end_arg = sys.argv[2] if len(sys.argv) > 2 else "2054-04-07 23:59:59"

    START = start_arg
    END = end_arg

    start_dt = pd.to_datetime(START)
    end_dt = pd.to_datetime(END)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    start_s = start_ms // 1000
    end_s = end_ms // 1000

    # Conecta ao banco e cria a tabela caso não exista
    conn = sqlite3.connect("funding_data.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS funding_detalhado (
            exchange TEXT, symbol TEXT, funding REAL, fees REAL, realized_pnl REAL,
            unrealized_pnl REAL, position_size REAL, pnl_total REAL,
            borrow_cost REAL, borrow_pnl REAL,
            start TIMESTAMP, end TIMESTAMP, executed_at TIMESTAMP
        )
    """)
    conn.commit()

    # Apaga registros anteriores dessa mesma janela (START igual)
    print(f"[INFO] Apagando dados existentes para janela com START = {START}...")
    cursor.execute("DELETE FROM funding_detalhado WHERE start = ?", (START,))
    conn.commit()

    print("[INFO] Binance...")
    b = parse_binance(fetch_binance_income(start_ms, end_ms))
    print("[INFO] Bybit...")
    y = parse_bybit(fetch_bybit_logs(start_ms, end_ms))
    print("[INFO] Gate.io...")
    g = parse_gateio(fetch_gateio_income(start_s, end_s))
    print("[INFO] Hyperliquid...")
    h = parse_hyper(
        fetch_hyperliquid_funding(HYPER_USER, start_ms, end_ms),
        fetch_hyperliquid_fills(HYPER_USER, start_ms, end_ms)
    )
    print("[INFO] CoinRoutes...")
    unrealized = fetch_coinroutes_positions()

    combined = b + y + g + h

    print("[DEBUG] Combined type check")
    for i, row in enumerate(combined):
        if not isinstance(row, dict):
            raise ValueError(f"Elemento {i} em combined não é dict")

    seen_keys = set()

    for row in combined:
        raw_symbol = row["symbol"]
        local_exchange = row["exchange"].lower()
        coin = extract_coin(raw_symbol)
        row["symbol"] = coin

        cr_exchange = EXCHANGE_MAP.get(local_exchange, local_exchange)
        key = (coin, cr_exchange)

        if key in unrealized and key not in seen_keys:
            row["unrealized_pnl"] = unrealized[key]["unrealized_pnl"]
            row["position_size"] = unrealized[key]["position_size"]
            seen_keys.add(key)

        row["pnl_total"] = (
            row["funding"] + row["fees"] + row["realized_pnl"] + row["unrealized_pnl"]
            - row.get("borrow_cost", 0.0) + row.get("borrow_pnl", 0.0)
        )

    df = pd.DataFrame(combined)
    df = df.sort_values(by=["symbol", "exchange"])

    df["start"] = START
    df["end"] = END
    df["executed_at"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    df.to_sql("funding_detalhado", conn, if_exists="append", index=False)
    conn.close()

    print("[INFO] Dados detalhados salvos na tabela 'funding_detalhado'.")
    df.to_csv("funding_fees.csv", index=False)

    # Garante que todas as colunas esperadas existam, mesmo que vazias
    for col in ["borrow_cost", "borrow_pnl", "funding", "fees", "realized_pnl", "unrealized_pnl", "position_size", "pnl_total"]:
        if col not in df.columns:
            df[col] = 0.0


    summary = df.groupby("symbol")[[
        "funding", "fees", "realized_pnl", "unrealized_pnl",
        "borrow_pnl", "borrow_cost", "pnl_total", "position_size"
    ]].sum()
    summary["roe_%"] = (summary["pnl_total"] / summary["position_size"]).replace([float("inf"), -float("inf")], 0) * 100

    print("\n[RESUMO]", summary["pnl_total"].sum())
    print(summary)
    print("\n[DETALHADO]")
    print(df)
    summary.to_csv("funding_fees_summary.csv")

if __name__ == "__main__":
    main()