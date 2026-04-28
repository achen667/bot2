from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class MarketSnapshot:
    ts: datetime
    market_slug: str
    up_bid: Optional[float]
    up_ask: Optional[float]
    down_bid: Optional[float]
    down_ask: Optional[float]
    underlying_price_now: Optional[float]
    price_to_beat: Optional[float]
    vol_60m: Optional[float]
    resolution_window_start: datetime
    resolution_window_end: datetime
    elapsed_fraction: Optional[float]
    time_to_expiry_sec: Optional[float]
    price_to_beat_source_timestamp: Optional[int] = None
    underlying_return_since_start: Optional[float] = None
    metadata: dict = field(default_factory=dict)


@dataclass
class SignalEvent:
    signal_id: str
    ts: datetime
    market_slug: str
    side: str
    pm_up_bid_at_signal: Optional[float]
    pm_up_ask_at_signal: Optional[float]
    pm_down_bid_at_signal: Optional[float]
    pm_down_ask_at_signal: Optional[float]
    pm_target_price_at_signal: Optional[float]
    fair_up_prob: float
    fair_down_prob: float
    edge_up: float
    edge_down: float
    trade_edge: float
    elapsed_fraction: float
    price_to_beat: float
    underlying_price_now: float
    underlying_return_since_start: float
    vol_60m: float
    return_z: float
    time_bucket_fine: str
    return_z_bucket: str
    lookup_fallback_level: int
    lookup_keys: str
    lookup_count: Optional[float]
    metadata: dict = field(default_factory=dict)


@dataclass
class OrderIntent:
    signal: SignalEvent
    side: str
    quantity: float


@dataclass
class OrderRecord:
    order_id: str
    signal_id: str
    market_slug: str
    side: str
    quantity: float
    submit_ts: datetime
    signal_ts: Optional[datetime] = None
    signal_price: Optional[float] = None
    status: str = "pending"
    fill_ts: Optional[datetime] = None
    fill_price: Optional[float] = None
    simulated: bool = True
    metadata: dict = field(default_factory=dict)


@dataclass
class PositionRecord:
    side: str
    market_slug: str
    quantity: float
    entry_order_id: str
    entry_signal_id: str
    entry_signal_ts: Optional[datetime]
    entry_signal_price: Optional[float]
    entry_fill_ts: datetime
    entry_price: float
    status: str = "open"
    exit_order_id: Optional[str] = None
    exit_fill_ts: Optional[datetime] = None
    exit_price: Optional[float] = None
    realized_pnl_bps: Optional[float] = None


@dataclass
class HeartbeatRecord:
    ts: datetime
    signals: int
    orders: int
    fills: int
    up_position_open: bool
    down_position_open: bool
    realized_pnl_bps_total: float
    unrealized_pnl_bps_total: float
