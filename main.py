from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from .clob_client_wrapper import ClobClientWrapper
from .config import BotConfig
from .execution_engine import ExecutionEngine
from .market_data import MarketDataService
from .models import HeartbeatRecord, OrderIntent
from .persistence import append_jsonl
from .portfolio_state import PortfolioState
from .risk_manager import RiskManager
from .signal_engine import SignalEngine


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)


async def run_bot(config: BotConfig | None = None) -> None:
    config = config or BotConfig()
    logger.info("Starting polymarketBot2 — paper=%s symbol=%s min_edge=%.4f", config.paper_trading, config.binance_symbol, config.min_edge)
    clob_client = None
    if not config.paper_trading:
        if config.keystore_path is None or config.gnosis_safe_address is None:
            raise ValueError("Live mode requires keystore_path and gnosis_safe_address")
        clob_client = ClobClientWrapper(str(config.keystore_path), config.gnosis_safe_address, config.clob_host, config.chain_id, response_log_path=config.clob_response_log_path)

    portfolio = PortfolioState()
    market_data = MarketDataService(config)
    signal_engine = SignalEngine(config)
    risk_manager = RiskManager(config, portfolio)
    execution_engine = ExecutionEngine(portfolio, market_data, config, clob_client)
    last_heartbeat = datetime.now(timezone.utc)
    tick_count = 0

    async for snapshot in market_data.stream():
        tick_count += 1
        signal = signal_engine.on_market_update(snapshot)
        if signal is not None:
            logger.info("Signal — side=%s fair_up=%.4f edge=%.4f z_bucket=%s lookup=%s count=%s", signal.side, signal.fair_up_prob, signal.trade_edge, signal.return_z_bucket, signal.lookup_keys, signal.lookup_count)
            portfolio.register_signal(signal)
            append_jsonl(config.signal_log_path, asdict(signal))
            intent = OrderIntent(signal=signal, side=signal.side, quantity=config.default_order_size)
            if risk_manager.allow(intent):
                asyncio.create_task(execution_engine.submit_market_order(intent))

        now = snapshot.ts
        if (now - last_heartbeat).total_seconds() >= config.heartbeat_interval_sec:
            heartbeat = HeartbeatRecord(
                ts=now,
                signals=len(portfolio.signal_log),
                orders=len(portfolio.order_log),
                fills=len(portfolio.fill_log),
                up_position_open=portfolio.has_open_exposure("BUY_UP"),
                down_position_open=portfolio.has_open_exposure("BUY_DOWN"),
                realized_pnl_bps_total=portfolio.realized_pnl_bps_total(),
                unrealized_pnl_bps_total=portfolio.unrealized_pnl_bps(snapshot),
            )
            append_jsonl(config.heartbeat_log_path, asdict(heartbeat))
            logger.info("Heartbeat — ticks=%d signals=%d orders=%d p2b=%s close=%s vol=%s", tick_count, heartbeat.signals, heartbeat.orders, snapshot.price_to_beat, snapshot.underlying_price_now, snapshot.vol_60m)
            last_heartbeat = now


def main() -> None:
    asyncio.run(run_bot())


if __name__ == "__main__":
    main()
