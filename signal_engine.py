from __future__ import annotations

from uuid import uuid4

from .config import BotConfig
from .fine_time_return_z_model import FineTimeReturnZModel
from .models import MarketSnapshot, SignalEvent


class SignalEngine:
    def __init__(self, config: BotConfig):
        self.config = config
        self.model = FineTimeReturnZModel(config.model_dir)
        self._traded_markets: set[str] = set()

    def on_market_update(self, snapshot: MarketSnapshot) -> SignalEvent | None:
        if self.config.first_signal_per_market and snapshot.market_slug in self._traded_markets:
            return None
        required = [
            snapshot.elapsed_fraction,
            snapshot.price_to_beat,
            snapshot.underlying_price_now,
            snapshot.vol_60m,
            snapshot.underlying_return_since_start,
            snapshot.up_ask,
            snapshot.down_ask,
        ]
        if any(value is None for value in required):
            return None
        elapsed = float(snapshot.elapsed_fraction)  # type: ignore[arg-type]
        if elapsed > self.config.early_phase_cutoff:
            return None
        elapsed_seconds = (snapshot.ts - snapshot.resolution_window_start).total_seconds()
        if elapsed_seconds < self.config.earliest_entry_seconds:
            return None

        pred = self.model.predict(
            elapsed_fraction=elapsed,
            underlying_return_since_start=float(snapshot.underlying_return_since_start),
            vol_60m=float(snapshot.vol_60m),
        )
        fair_up = pred.fair_up_prob
        fair_down = 1.0 - fair_up
        edge_up = fair_up - float(snapshot.up_ask)
        edge_down = fair_down - float(snapshot.down_ask)
        if edge_up >= edge_down:
            side = "BUY_UP"
            target_price = snapshot.up_ask
            trade_edge = edge_up
        else:
            side = "BUY_DOWN"
            target_price = snapshot.down_ask
            trade_edge = edge_down
        if trade_edge < self.config.min_edge:
            return None

        signal = SignalEvent(
            signal_id=str(uuid4()),
            ts=snapshot.ts,
            market_slug=snapshot.market_slug,
            side=side,
            pm_up_bid_at_signal=snapshot.up_bid,
            pm_up_ask_at_signal=snapshot.up_ask,
            pm_down_bid_at_signal=snapshot.down_bid,
            pm_down_ask_at_signal=snapshot.down_ask,
            pm_target_price_at_signal=target_price,
            fair_up_prob=fair_up,
            fair_down_prob=fair_down,
            edge_up=edge_up,
            edge_down=edge_down,
            trade_edge=trade_edge,
            elapsed_fraction=elapsed,
            price_to_beat=float(snapshot.price_to_beat),
            underlying_price_now=float(snapshot.underlying_price_now),
            underlying_return_since_start=float(snapshot.underlying_return_since_start),
            vol_60m=float(snapshot.vol_60m),
            return_z=pred.return_z,
            time_bucket_fine=pred.time_bucket_fine,
            return_z_bucket=pred.return_z_bucket,
            lookup_fallback_level=pred.fallback_level,
            lookup_keys=pred.keys,
            lookup_count=pred.count,
            metadata={"price_to_beat_source_timestamp": snapshot.price_to_beat_source_timestamp},
        )
        self._traded_markets.add(snapshot.market_slug)
        return signal
