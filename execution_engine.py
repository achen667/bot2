from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import datetime, timezone
from uuid import uuid4

from .config import BotConfig
from .market_data import MarketDataService
from .models import OrderIntent, OrderRecord, PositionRecord
from .persistence import append_jsonl
from .portfolio_state import PortfolioState

logger = logging.getLogger(__name__)


class ExecutionEngine:
    def __init__(self, portfolio: PortfolioState, market_data: MarketDataService, config: BotConfig, clob_client=None) -> None:
        self.portfolio = portfolio
        self.market_data = market_data
        self.config = config
        self.clob_client = clob_client

    def _token_id_for_side(self, side: str) -> str:
        meta = self.market_data._market_meta or {}
        if side == "BUY_UP":
            return meta["yes_token_id"]
        if side == "BUY_DOWN":
            return meta["no_token_id"]
        raise ValueError(f"Unknown side: {side}")

    def _current_bbo(self, side: str):
        snapshot = self.market_data.get_latest_snapshot()
        if snapshot is None:
            return None, None
        return (snapshot.up_bid, snapshot.up_ask) if side == "BUY_UP" else (snapshot.down_bid, snapshot.down_ask)

    async def submit_market_order(self, intent: OrderIntent) -> None:
        side = intent.side
        signal = intent.signal
        signal_id = signal.signal_id
        usd_amount = float(intent.quantity)
        self.portfolio.mark_pending(side)
        try:
            submit_ts = datetime.now(timezone.utc)
            order_id = str(uuid4())
            _, ask = self._current_bbo(side)
            entry_price = float(ask if ask is not None else signal.pm_target_price_at_signal)

            if self.config.paper_trading:
                shares = usd_amount / entry_price if entry_price > 0 else 0.0
                order = OrderRecord(
                    order_id=order_id,
                    signal_id=signal_id,
                    market_slug=signal.market_slug,
                    side=side,
                    quantity=usd_amount,
                    submit_ts=submit_ts,
                    signal_ts=signal.ts,
                    signal_price=signal.pm_target_price_at_signal,
                    status="paper_filled",
                    fill_ts=submit_ts,
                    fill_price=entry_price,
                    simulated=True,
                    metadata=asdict(signal),
                )
                position = PositionRecord(
                    side=side,
                    market_slug=signal.market_slug,
                    quantity=shares,
                    entry_order_id=order_id,
                    entry_signal_id=signal_id,
                    entry_signal_ts=signal.ts,
                    entry_signal_price=signal.pm_target_price_at_signal,
                    entry_fill_ts=submit_ts,
                    entry_price=entry_price,
                )
                self.portfolio.register_order(order)
                self.portfolio.register_fill(order)
                self.portfolio.open_position(position)
                append_jsonl(self.config.order_log_path, asdict(order))
                append_jsonl(self.config.fill_log_path, asdict(order))
                append_jsonl(
                    self.config.paper_trade_log_path,
                    {
                        "event": "paper_entry",
                        "order": asdict(order),
                        "position": asdict(position),
                        "signal": asdict(signal),
                    },
                )
                logger.info("Paper filled — side=%s entry=%.4f edge=%.4f market=%s", side, entry_price, signal.trade_edge, signal.market_slug)
                return

            if self.clob_client is None:
                raise RuntimeError("Live trading requested but clob_client is not initialized")
            response = await self.clob_client.submit_order(
                token_id=self._token_id_for_side(side),
                price=self.config.market_buy_price,
                size=usd_amount,
                side="BUY",
            )
            order = OrderRecord(order_id=response.get("orderID") or response.get("id") or order_id, signal_id=signal_id, market_slug=signal.market_slug, side=side, quantity=usd_amount, submit_ts=submit_ts, signal_ts=signal.ts, signal_price=signal.pm_target_price_at_signal, status=str(response.get("status", "unknown")), fill_ts=datetime.now(timezone.utc), fill_price=entry_price, simulated=False, metadata={"response": response, "signal": asdict(signal)})
            self.portfolio.register_order(order)
            append_jsonl(self.config.order_log_path, asdict(order))
        finally:
            self.portfolio.clear_pending(side)
