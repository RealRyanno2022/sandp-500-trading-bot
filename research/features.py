import pandas as pd
import numpy as np

def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute daily alpha-style indicators.
    """
    df = df.copy().set_index("ts").sort_index()
    px = df["c"]

    # returns
    df["ret_1d"] = px.pct_change()
    df["ret_5d"] = px.pct_change(5)
    df["ret_21d"] = px.pct_change(21)

    # moving averages
    df["sma_10"] = px.rolling(10).mean()
    df["sma_20"] = px.rolling(20).mean()
    df["ema_12"] = px.ewm(span=12, adjust=False).mean()
    df["ema_26"] = px.ewm(span=26, adjust=False).mean()
    df["macd"] = df["ema_12"] - df["ema_26"]
    df["macd_sig"] = df["macd"].ewm(span=9, adjust=False).mean()

    # volatility
    tr = pd.concat([
        (df["h"] - df["l"]).abs(),
        (df["h"] - df["c"].shift()).abs(),
        (df["l"] - df["c"].shift()).abs()
    ], axis=1).max(axis=1)
    df["atr_14"] = tr.rolling(14).mean()
    df["vol_21d"] = df["ret_1d"].rolling(21).std() * np.sqrt(252)

    # RSI
    delta = px.diff()
    up = delta.clip(lower=0).rolling(14).mean()
    down = (-delta.clip(upper=0)).rolling(14).mean()
    rs = up / (down.replace(0, np.nan))
    df["rsi_14"] = 100 - (100 / (1 + rs))

    # volume
    df["vol_sma_20"] = df["v"].rolling(20).mean()
    df["vol_rel"] = df["v"] / df["vol_sma_20"]

    return df.dropna().reset_index()
