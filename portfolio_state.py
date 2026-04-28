from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

from .models import MarketSnapshot, OrderRecord, PositionRecord, SignalEvent


class PortfolioState:
    def __init__(self) -> None:
        self.open_positions: List[PositionRecord] = []
        self._closed_positions: List[PositionRecord] = []
        self._pending_count: Dict[str, int] = {}
        self.signal_log: List[SignalEvent] = []
        self.order_log: List[OrderRecord] = []
        self.fill_log: List[OrderRecord] = []

    def open_exposure_count(self, side: str) -> int:
        return sum(1 for p in self.open_positions if p.side == side)

    def pending_order_count(self, side: str) -> int:
        return self._pending_count.get(side, 0)

    def has_open_exposure(self, side: str) -> bool:
        return self.open_exposure_count(side) > 0

    def get_open_position(self, signal_id: str) -> Optional[PositionRecord]:
        return next((p for p in self.open_positions if p.entry_signal_id == signal_id), None)

    def mark_pending(self, side: str) -> None:
        self._pending_count[side] = self._pending_count.get(side, 0) + 1

    def clear_pending(self, side: str) -> None:
        current = self._pending_count.get(side, 0)
        if current > 1:
            self._pending_count[side] = current - 1
        else:
            self._pending_count.pop(side, None)

    def register_signal(self, signal: SignalEvent) -> None:
        self.signal_log.append(signal)

    def register_order(self, order: OrderRecord) -> None:
        self.order_log.append(order)

    def register_fill(self, order: OrderRecord) -> None:
        self.fill_log.append(order)

    def open_position(self, position: PositionRecord) -> None:
        self.open_positions.append(position)

    def close_position(self, signal_id: str, exit_order_id: str, exit_fill_ts: datetime, exit_price: float) -> Optional[PositionRecord]:
        for i, pos in enumerate(self.open_positions):
            if pos.entry_signal_id == signal_id:
                self.open_positions.pop(i)
                pos.exit_order_id = exit_order_id
                pos.exit_fill_ts = exit_fill_ts
                pos.exit_price = exit_price
                pos.status = "closed"
                if pos.entry_price > 0:
                    pos.realized_pnl_bps = (exit_price - pos.entry_price) / pos.entry_price * 1e4
                self._closed_positions.append(pos)
                return pos
        return None

    def realized_pnl_bps_total(self) -> float:
        return sum(p.realized_pnl_bps or 0.0 for p in self._closed_positions)

    def unrealized_pnl_bps(self, snapshot: MarketSnapshot) -> float:
        total = 0.0
        for pos in self.open_positions:
            mark = snapshot.up_bid if pos.side == "BUY_UP" else snapshot.down_bid if pos.side == "BUY_DOWN" else None
            if mark is not None and pos.entry_price > 0:
                total += (mark - pos.entry_price) / pos.entry_price * 1e4
        return total
