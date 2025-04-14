# perp-arbv2

A funding arbitrage system using **CoinRoutes** and **Coinglass** for real-time funding rates and market data.

## 🧠 Overview

This system is designed to identify and act on funding arbitrage opportunities across multiple cryptocurrency exchanges. It consists of multiple interconnected modules that handle everything from data collection to trade execution and PnL tracking.

---

## ⚙️ Modules

### 1. `executor.py`
- Uses **`ccxt.async_support`** to send taker-taker orders on different exchanges.
- Manages position limits, retries, dry-run mode, and logs all executions in detail.

### 2. `stream.py`
- Streams **price and funding rate data** from CoinRoutes and Coinglass.
- Updates tick-by-tick order book snapshots and funding info for top symbols.

### 3. `symbol_filter.py`
- Selects which symbols to monitor and trade based on funding spread, liquidity, and exchange availability.

### 4. `spread_analyzer.py`
- Calculates **funding spreads** and identifies valid arbitrage opportunities.
- Takes into account taker fees and expected carry to ensure positive edge.

### 5. `logs.py`
- Aggregates and analyzes:
  - Funding received/paid per asset and day
  - Trading fees
  - **Realized** and **unrealized PnL**
- Generates detailed reports for performance evaluation and auditing.

---

## 📦 Requirements

- Python 3.10+
- `ccxt[async_support]`
- Redis
- CoinRoutes API key (if private tier is used)
- Coinglass API key

---

## 🚀 Getting Started

```bash
git clone https://github.com/yourusername/perp-arbv2.git
cd perp-arbv2
pip install -r requirements.txt
