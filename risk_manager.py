from __future__ import annotations

import time

from .config import BotConfig
from .models import OrderIntent
from .portfolio_state import PortfolioState


class RiskManager:
    def __init__(self, config: BotConfig, portfolio: PortfolioState):
        self.config = config
        self.portfolio = portfolio
        self._last_accepted_ts_ms = 0.0

    def allow(self, intent: OrderIntent) -> bool:
        if self.config.signal_cooldown_ms > 0:
            now_ms = time.monotonic() * 1000
            if now_ms - self._last_accepted_ts_ms < self.config.signal_cooldown_ms:
                return False
        limit = self.config.max_positions_per_side
        if limit > 0:
            total = self.portfolio.open_exposure_count(intent.side) + self.portfolio.pending_order_count(intent.side)
            if total >= limit:
                return False
        self._last_accepted_ts_ms = time.monotonic() * 1000
        return True
