from dataclasses import dataclass

@dataclass
class ScalperConfig:
    symbol: str = "ES"          # "ES" or "MES"
    use_mes: bool = False       # True -> MES sizing ($5/tick)
    tz: str = "America/New_York"

    # session
    start_time: str = "09:35"
    stop_time:  str = "15:45"

    # signals
    min_atr: float = 0.60       # in points (1-min ATR guardrails)
    max_atr: float = 5.00
    ema_fast: int = 9
    ema_slow: int = 21

    # exits (ticks)
    tp_ticks: int = 4           # 1.00 pt on ES
    sl_ticks: int = 6           # 1.50 pt on ES
    be_trigger_ticks: int = 2   # move stop to BE+1 after +2 ticks
    time_stop_sec: int = 180

    # money management
    risk_pct: float = 0.002     # 0.2% of NLV per trade
    risk_abs_cap: float = 150.0 # hard cap per trade in $
    max_daily_loss: float = 600.0
    max_trades: int = 12

    # housekeeping
    allow_news_pause: bool = False
    news_pause_min: int = 10
