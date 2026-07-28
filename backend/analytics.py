import datetime
import pandas as pd
from typing import Dict, Any, Callable, List, Optional
from backend.database import get_db_connection

# =====================================================================
# INDICATOR REGISTRY & DECORATOR PATTERN
# =====================================================================

INDICATOR_REGISTRY: Dict[str, Callable[[pd.DataFrame, Dict[str, Any]], pd.DataFrame]] = {}

def register_indicator(name: str):
    """
    Decorator that automatically registers an indicator strategy function into the system.
    """
    def decorator(func: Callable[[pd.DataFrame, Dict[str, Any]], pd.DataFrame]):
        INDICATOR_REGISTRY[name] = func
        return func
    return decorator


# =====================================================================
# INDIVIDUAL INDICATOR PLUGINS
# =====================================================================

@register_indicator("ema")
def compute_ema(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Computes Exponential Moving Average (EMA)."""
    period = params.get("period", 20)
    col_name = f"ema_{period}"
    df[col_name] = df['close'].ewm(span=period, adjust=False).mean()
    return df


@register_indicator("rsi")
def compute_rsi(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Computes Relative Strength Index (RSI) using Wilder's Smoothing."""
    period = params.get("period", 14)
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss
    df[f'rsi_{period}'] = 100 - (100 / (1 + rs))
    return df


@register_indicator("macd")
def compute_macd(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Computes Moving Average Convergence Divergence (MACD)."""
    fast = params.get("fast", 12)
    slow = params.get("slow", 26)
    signal = params.get("signal", 9)

    ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow, adjust=False).mean()

    df['macd_line'] = ema_fast - ema_slow
    df['macd_signal'] = df['macd_line'].ewm(span=signal, adjust=False).mean()
    df['macd_hist'] = df['macd_line'] - df['macd_signal']
    return df


@register_indicator("bollinger_bands")
def compute_bollinger_bands(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Computes Bollinger Bands."""
    period = params.get("period", 20)
    std_dev = params.get("std_dev", 2)

    sma = df['close'].rolling(window=period).mean()
    rolling_std = df['close'].rolling(window=period).std()

    df['bb_middle'] = sma
    df['bb_upper'] = sma + (rolling_std * std_dev)
    df['bb_lower'] = sma - (rolling_std * std_dev)
    return df


# =====================================================================
# DATA FETCHING & DYNAMIC PIPELINE ENGINE
# =====================================================================

async def fetch_ohlc_dataframe(
    stock_code: str, 
    interval: str = "1min", 
    lookback_days: int = 30
) -> pd.DataFrame:
    """
    Fetches candle data from SQLite and returns a clean, sorted Pandas DataFrame.
    Includes a lookback window to allow proper indicator warm-up.
    """
    start_date = (
        datetime.datetime.now() - datetime.timedelta(days=lookback_days)
    ).strftime("%Y-%m-%d")

    query = """
        SELECT timestamp, open, high, low, close, volume
        FROM ohlc_candles
        WHERE stock_code = ? AND interval = ? AND timestamp >= ?
        ORDER BY timestamp ASC
    """
    
    async with await get_db_connection() as db:
        async with db.execute(query, (stock_code, interval, start_date)) as cursor:
            rows = await cursor.fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([dict(r) for r in rows])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df


def calculate_technical_indicators(
    df: pd.DataFrame, 
    configs: Optional[List[Dict[str, Any]]] = None
) -> pd.DataFrame:
    """
    Executes a list of requested indicator strategies dynamically over the DataFrame.
    
    Example Config:
    [
        {"name": "ema", "params": {"period": 20}},
        {"name": "rsi", "params": {"period": 14}},
        {"name": "macd", "params": {"fast": 12, "slow": 26, "signal": 9}}
    ]
    """
    if df.empty:
        return df

    # Default indicators if none specified
    if configs is None:
        configs = [
            {"name": "ema", "params": {"period": 20}},
            {"name": "rsi", "params": {"period": 14}}
        ]

    for config in configs:
        indicator_name = config.get("name")
        params = config.get("params", {})

        if indicator_name in INDICATOR_REGISTRY:
            df = INDICATOR_REGISTRY[indicator_name](df, params)

    return df


async def get_order_imbalance(stock_code: str) -> Optional[Dict[str, Any]]:
    """
    Calculates Order Book Imbalance (OBI) from the latest tick frame.
    Returns a normalized float between -1.0 (Heavy Sell) and +1.0 (Heavy Buy).
    """
    query = """
        SELECT best_bid_price, best_bid_qty, best_ask_price, best_ask_qty, last_price, timestamp
        FROM ticks
        WHERE stock_code = ?
        ORDER BY id DESC LIMIT 1
    """
    
    async with await get_db_connection() as db:
        async with db.execute(query, (stock_code,)) as cursor:
            row = await cursor.fetchone()

    if not row or not row["best_bid_qty"] or not row["best_ask_qty"]:
        return None

    bid_qty = row["best_bid_qty"]
    ask_qty = row["best_ask_qty"]
    total_depth = bid_qty + ask_qty

    imbalance = (bid_qty - ask_qty) / total_depth if total_depth > 0 else 0.0

    return {
        "stock_code": stock_code,
        "last_price": row["last_price"],
        "best_bid_price": row["best_bid_price"],
        "best_ask_price": row["best_ask_price"],
        "imbalance": round(imbalance, 4),
        "timestamp": row["timestamp"]
    }