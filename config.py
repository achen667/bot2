from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class BotConfig:
    # Safety first: paper mode is the default.
    paper_trading: bool = True
    default_order_size: float = 1.0
    max_positions_per_side: int = 1
    signal_cooldown_ms: int = 0
    first_signal_per_market: bool = True

    # Market/model controls.
    market_type: str = "5m"
    interval_minutes: int = 5
    market_base_ts: int = 1775391900
    binance_symbol: str = "SOLUSDT"
    chainlink_symbol: Optional[str] = None
    model_dir: Path = Path("polymarketBot2/model_artifacts")
    min_edge: float = 0.0
    early_phase_cutoff: float = 0.35
    earliest_entry_seconds: float = 3

    # Network endpoints.
    gamma_url: str = "https://gamma-api.polymarket.com/markets"
    binance_rest_base: str = "https://fapi.binance.com"
    binance_ws_base: str = "wss://fstream.binance.com/stream"
    polymarket_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    rtds_ws_url: str = "wss://ws-live-data.polymarket.com"

    # Optional static market override.
    market_slug: Optional[str] = None
    yes_token_id: Optional[str] = None
    no_token_id: Optional[str] = None

    # Runtime/logging.
    heartbeat_interval_sec: int = 5
    price_to_beat_grace_sec: float = 20.0
    signal_log_path: Path = Path("polymarketBot2/runtime/signal_log.jsonl")
    order_log_path: Path = Path("polymarketBot2/runtime/order_log.jsonl")
    fill_log_path: Path = Path("polymarketBot2/runtime/fill_log.jsonl")
    heartbeat_log_path: Path = Path("polymarketBot2/runtime/heartbeat_log.jsonl")
    paper_trade_log_path: Path = Path("polymarketBot2/runtime/paper_trades.jsonl")
    price_to_beat_log_path: Path = Path("polymarketBot2/runtime/price_to_beat_data.json")

    # Live trading, optional.
    clob_host: str = "https://clob.polymarket.com"
    chain_id: int = 137
    keystore_path: Optional[Path] = Path("polymarketBot2/keystore.json")
    gnosis_safe_address: Optional[str] = None
    market_buy_price: float = 0.99
    market_sell_price: float = 0.01
    clob_response_log_path: Path = Path("polymarketBot2/runtime/clob_response_log.jsonl")
