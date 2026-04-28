from __future__ import annotations

import asyncio
import json
import logging
import math
import re
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Deque, Dict, Optional, Tuple

import aiohttp

from .config import BotConfig
from .models import MarketSnapshot
from .price_to_beat import PriceToBeatRecord, PriceToBeatStore, bucket_start_ms, extract_records, normalize_symbol

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp_to_datetime(timestamp: object) -> Optional[datetime]:
    if timestamp is None:
        return None
    try:
        ts = int(timestamp)
    except (TypeError, ValueError):
        return None
    # Polymarket timestamps may arrive in seconds while some feeds use ms.
    if ts >= 1_000_000_000_000:
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def _best_bid(levels) -> Optional[float]:
    prices = [float(level["price"]) for level in levels if "price" in level]
    return max(prices) if prices else None


def _best_ask(levels) -> Optional[float]:
    prices = [float(level["price"]) for level in levels if "price" in level]
    return min(prices) if prices else None


def _parse_json_array(value) -> list:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return []
    return []


@dataclass
class BinanceKlineState:
    ts: datetime
    close: float
    vol_60m: Optional[float]


@dataclass
class PolymarketState:
    ts: datetime
    market_slug: str
    up_bid: Optional[float]
    up_ask: Optional[float]
    down_bid: Optional[float]
    down_ask: Optional[float]


class MarketDataService:
    def __init__(self, config: BotConfig):
        self.config = config
        self._latest_binance: Optional[BinanceKlineState] = None
        self._latest_polymarket: Optional[PolymarketState] = None
        self._latest_price_to_beat: Optional[PriceToBeatRecord] = None
        self._close_buf: Deque[float] = deque(maxlen=61)
        self._chainlink_buf: Deque[Tuple[float, PriceToBeatRecord]] = deque()
        self._active_market_start_ts = self.compute_active_market_start_ts()
        self._market_meta = None
        self.price_store = PriceToBeatStore(config.price_to_beat_log_path)
        self.chainlink_symbol = normalize_symbol(config.chainlink_symbol or config.binance_symbol)
        self._last_waiting_p2b_bucket_ms: Optional[int] = None
        self._last_rtds_unparsed_log_ts = 0.0
        self._last_p2b_wait_log_ts = 0.0

    def _prune_chainlink_buffer(self, now_ts: Optional[float] = None) -> None:
        now_ts = now_ts if now_ts is not None else datetime.now(timezone.utc).timestamp()
        while self._chainlink_buf and now_ts - self._chainlink_buf[0][0] > 5.0:
            self._chainlink_buf.popleft()

    def _buffer_chainlink_record(self, record: PriceToBeatRecord) -> None:
        now_ts = datetime.now(timezone.utc).timestamp()
        self._chainlink_buf.append((now_ts, record))
        self._prune_chainlink_buffer(now_ts)

    def _lookup_buffered_record(self, bucket_ms: int) -> Optional[PriceToBeatRecord]:
        self._prune_chainlink_buffer()
        for _, record in reversed(self._chainlink_buf):
            if record.symbol == self.chainlink_symbol and record.bucket_start_ms == bucket_ms:
                return record
        return None

    def interval_seconds(self) -> int:
        return int(self.config.interval_minutes * 60)

    def compute_active_market_start_ts(self, now: Optional[datetime] = None) -> int:
        now = now or _utc_now()
        now_ts = int(now.timestamp())
        base_ts = int(self.config.market_base_ts)
        step = self.interval_seconds()
        return base_ts if now_ts <= base_ts else ((now_ts - base_ts) // step) * step + base_ts

    async def _resolve_market_meta(self, session: aiohttp.ClientSession, market_start_ts: int) -> Dict[str, str]:
        if self.config.market_slug and self.config.yes_token_id and self.config.no_token_id:
            return {"market_slug": self.config.market_slug, "yes_token_id": self.config.yes_token_id, "no_token_id": self.config.no_token_id}
        slug_asset = self.config.binance_symbol.lower().replace("usdt", "")
        slug = f"{slug_asset}-updown-{self.config.market_type}-{market_start_ts}"
        params = {"slug": slug}
        async with session.get(self.config.gamma_url, params=params, timeout=15) as resp:
            resp.raise_for_status()
            payload = await resp.json()
        record = payload[0] if isinstance(payload, list) and payload else payload
        if not isinstance(record, dict):
            raise RuntimeError(f"Gamma returned no market metadata for slug={slug}")
        outcomes = [str(x).lower() for x in _parse_json_array(record.get("outcomes"))]
        clob_ids = [str(x) for x in _parse_json_array(record.get("clobTokenIds"))]
        if len(clob_ids) < 2:
            raise RuntimeError(f"Gamma metadata missing clobTokenIds for slug={slug}")
        up_idx = next((i for i, x in enumerate(outcomes) if x in {"up", "yes"}), 0)
        down_idx = next((i for i, x in enumerate(outcomes) if x in {"down", "no"}), 1)
        return {"market_slug": str(record.get("slug") or slug), "yes_token_id": clob_ids[up_idx], "no_token_id": clob_ids[down_idx]}

    async def _refresh_active_market(self, session: aiohttp.ClientSession, now: Optional[datetime] = None) -> bool:
        start_ts = self.compute_active_market_start_ts(now)
        if self._market_meta is not None and start_ts == self._active_market_start_ts:
            return False
        self._active_market_start_ts = start_ts
        self._market_meta = await self._resolve_market_meta(session, start_ts)
        self._latest_polymarket = None
        self._latest_price_to_beat = None
        self._last_waiting_p2b_bucket_ms = None
        self._last_p2b_wait_log_ts = 0.0
        buffered = self._lookup_buffered_record(start_ts * 1000)
        if buffered is not None:
            self._latest_price_to_beat = buffered
        logger.info("Active market resolved — slug=%s", self._market_meta["market_slug"])
        return True

    def get_latest_snapshot(self) -> Optional[MarketSnapshot]:
        return self._build_snapshot()

    async def stream(self):
        queue: asyncio.Queue[Tuple[str, object]] = asyncio.Queue()
        async with aiohttp.ClientSession() as session:
            await self._refresh_active_market(session)
            await self._bootstrap_binance_klines(session)
            tasks = [
                asyncio.create_task(self._run_binance_kline_ws(session, queue)),
                asyncio.create_task(self._run_price_to_beat_ws(session, queue)),
                asyncio.create_task(self._run_polymarket_ws(session, queue)),
            ]
            try:
                while True:
                    source, payload = await queue.get()
                    if source == "binance":
                        self._latest_binance = payload  # type: ignore[assignment]
                    elif source == "price_to_beat":
                        self._latest_price_to_beat = payload  # type: ignore[assignment]
                    elif source == "polymarket":
                        self._latest_polymarket = payload  # type: ignore[assignment]
                    snapshot = self._build_snapshot()
                    if snapshot is not None:
                        yield snapshot
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    def _build_snapshot(self) -> Optional[MarketSnapshot]:
        if self._latest_binance is None or self._latest_polymarket is None or self._market_meta is None:
            return None
        ts = max(self._latest_binance.ts, self._latest_polymarket.ts)
        start = datetime.fromtimestamp(self._active_market_start_ts, tz=timezone.utc)
        end = datetime.fromtimestamp(self._active_market_start_ts + self.interval_seconds(), tz=timezone.utc)
        bucket_ms = bucket_start_ms(int(self._latest_polymarket.ts.timestamp() * 1000))
        p2b = self.price_store.get(self.chainlink_symbol, bucket_ms)
        buffered_p2b = self._lookup_buffered_record(bucket_ms)
        if buffered_p2b is not None:
            p2b = buffered_p2b
        if self._latest_price_to_beat is not None and self._latest_price_to_beat.bucket_start_ms == bucket_ms:
            p2b = self._latest_price_to_beat
        if p2b is None or p2b.bucket_start_ms != bucket_ms:
            now = datetime.now(timezone.utc)
            if self._last_waiting_p2b_bucket_ms != bucket_ms or now.timestamp() - self._last_p2b_wait_log_ts >= 2:
                latest_seen = self._latest_price_to_beat.source_timestamp if self._latest_price_to_beat else None
                logger.info(
                    "Waiting for minute-aligned price_to_beat — symbol=%s required_bucket_ms=%d market_ts=%s latest_seen_source_ts=%s store_records=%d",
                    self.chainlink_symbol,
                    bucket_ms,
                    self._latest_polymarket.ts.isoformat(),
                    latest_seen,
                    len(self.price_store.records),
                )
                self._last_waiting_p2b_bucket_ms = bucket_ms
                self._last_p2b_wait_log_ts = now.timestamp()
            return None
        elapsed = (ts - start).total_seconds() / max((end - start).total_seconds(), 1.0)
        elapsed = min(max(elapsed, 0.0), 1.0)
        ret = self._latest_binance.close / p2b.price_to_beat - 1.0 if p2b.price_to_beat > 0 else None
        return MarketSnapshot(
            ts=ts,
            market_slug=self._latest_polymarket.market_slug,
            up_bid=self._latest_polymarket.up_bid,
            up_ask=self._latest_polymarket.up_ask,
            down_bid=self._latest_polymarket.down_bid,
            down_ask=self._latest_polymarket.down_ask,
            underlying_price_now=self._latest_binance.close,
            price_to_beat=p2b.price_to_beat,
            vol_60m=self._latest_binance.vol_60m,
            resolution_window_start=start,
            resolution_window_end=end,
            elapsed_fraction=elapsed,
            time_to_expiry_sec=float((end - ts).total_seconds()),
            price_to_beat_source_timestamp=p2b.source_timestamp,
            underlying_return_since_start=ret,
            metadata={"binance_kline_ts": self._latest_binance.ts.isoformat(), "price_to_beat_bucket": p2b.bucket_start},
        )

    async def _bootstrap_binance_klines(self, session: aiohttp.ClientSession) -> None:
        """Seed close buffer from completed Binance 1m klines before websocket updates arrive.

        This matches the backtest口径: the model sees the latest completed 1m close
        at or before the Polymarket snapshot, not the live mid inside the current
        minute. REST may include an in-progress candle, so we explicitly keep only
        rows whose close time is already in the past.
        """

        url = f"{self.config.binance_rest_base}/fapi/v1/klines"
        params = {"symbol": self.config.binance_symbol.upper(), "interval": "1m", "limit": "70"}
        try:
            async with session.get(url, params=params, timeout=10) as resp:
                resp.raise_for_status()
                rows = await resp.json()
        except Exception as exc:
            logger.warning("Binance kline bootstrap failed %s: %s; waiting for websocket candles", type(exc).__name__, exc)
            return

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        completed: list[tuple[int, float]] = []
        for row in rows:
            try:
                close_time_ms = int(row[6])
                close = float(row[4])
            except (TypeError, ValueError, IndexError):
                continue
            if close_time_ms < now_ms and close > 0:
                completed.append((close_time_ms, close))

        if not completed:
            logger.warning("Binance kline bootstrap returned no completed candles")
            return

        self._close_buf.clear()
        for _, close in completed[-61:]:
            self._close_buf.append(close)
        last_close_time_ms, last_close = completed[-1]
        vol = self._compute_vol_60m()
        self._latest_binance = BinanceKlineState(
            ts=datetime.fromtimestamp(last_close_time_ms / 1000, tz=timezone.utc),
            close=last_close,
            vol_60m=vol,
        )
        logger.info(
            "Bootstrapped Binance 1m closes — candles=%d last_close=%.6f vol_60m=%s",
            len(self._close_buf),
            last_close,
            vol,
        )

    async def _run_binance_kline_ws(self, session: aiohttp.ClientSession, queue: asyncio.Queue) -> None:
        symbol = self.config.binance_symbol.lower()
        url = f"{self.config.binance_ws_base}?streams={symbol}@kline_1m"
        while True:
            try:
                async with session.ws_connect(url, heartbeat=20) as ws:
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        payload = json.loads(msg.data).get("data", {})
                        k = payload.get("k", {})
                        if not k.get("x"):
                            continue
                        close = float(k["c"])
                        close_time_ms = int(k["T"])
                        self._close_buf.append(close)
                        vol = self._compute_vol_60m()
                        await queue.put(("binance", BinanceKlineState(datetime.fromtimestamp(close_time_ms / 1000, tz=timezone.utc), close, vol)))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Binance kline WS error %s: %s; reconnecting", type(exc).__name__, exc)
                await asyncio.sleep(3)

    def _compute_vol_60m(self) -> Optional[float]:
        closes = list(self._close_buf)
        if len(closes) < 21:
            return None
        returns = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes)) if closes[i - 1] > 0]
        returns = returns[-60:]
        if len(returns) < 20:
            return None
        mean = sum(returns) / len(returns)
        var = sum((x - mean) ** 2 for x in returns) / (len(returns) - 1)
        return math.sqrt(var)

    async def _run_price_to_beat_ws(self, session: aiohttp.ClientSession, queue: asyncio.Queue) -> None:
        subscribe_message = {"action": "subscribe", "subscriptions": [{"topic": "crypto_prices_chainlink", "type": "*", "filters": json.dumps({"symbol": self.chainlink_symbol})}]}
        while True:
            try:
                logger.info("Connecting RTDS price_to_beat websocket — url=%s symbol=%s", self.config.rtds_ws_url, self.chainlink_symbol)
                async with session.ws_connect(self.config.rtds_ws_url, heartbeat=20) as ws:
                    await ws.send_json(subscribe_message)
                    logger.info("Subscribed RTDS price_to_beat — topic=crypto_prices_chainlink symbol=%s", self.chainlink_symbol)
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        raw = msg.data
                        if not raw or not raw.strip():
                            continue
                        try:
                            message = json.loads(raw)
                        except json.JSONDecodeError:
                            logger.debug("Ignoring non-JSON RTDS message: %r", raw[:200])
                            continue
                        records = [rec for rec in extract_records(message, default_symbol=self.chainlink_symbol) if rec.symbol == self.chainlink_symbol]
                        if not records:
                            now_ts = datetime.now(timezone.utc).timestamp()
                            if now_ts - self._last_rtds_unparsed_log_ts >= 30:
                                logger.info("RTDS message did not produce matching price_to_beat — expected_symbol=%s sample=%r", self.chainlink_symbol, raw[:500])
                                self._last_rtds_unparsed_log_ts = now_ts
                            continue
                        current_bucket_ms = self._active_market_start_ts * 1000
                        current_minute_record: Optional[PriceToBeatRecord] = None
                        saved_count = 0
                        saved_buckets: list[int] = []
                        for rec in records:
                            self._buffer_chainlink_record(rec)
                            saved = self.price_store.add(rec)
                            if saved:
                                saved_count += 1
                                saved_buckets.append(rec.bucket_start_ms)
                                logger.info(
                                    "Captured minute price_to_beat — symbol=%s bucket=%s price=%.8f source_ts=%s saved=%s",
                                    rec.symbol,
                                    rec.bucket_start,
                                    rec.price_to_beat,
                                    rec.source_timestamp,
                                    saved,
                                )
                            if rec.bucket_start_ms == current_bucket_ms:
                                current_minute_record = rec
                        if saved_count:
                            logger.info("Processed RTDS price batch — records=%d saved_buckets=%s saved=%d", len(records), saved_buckets, saved_count)
                        if current_minute_record is not None:
                            await queue.put(("price_to_beat", current_minute_record))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("RTDS price_to_beat WS error %s: %s; reconnecting", type(exc).__name__, exc)
                await asyncio.sleep(3)

    async def _run_polymarket_ws(self, session: aiohttp.ClientSession, queue: asyncio.Queue) -> None:
        while True:
            try:
                await self._refresh_active_market(session)
                token_ids = [self._market_meta["yes_token_id"], self._market_meta["no_token_id"]]  # type: ignore[index]
                market_slug = self._market_meta["market_slug"]  # type: ignore[index]
                latest: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
                async with session.ws_connect(self.config.polymarket_ws_url, heartbeat=20) as ws:
                    await ws.send_json({"type": "market", "assets_ids": token_ids})
                    async for msg in ws:
                        market_changed = await self._refresh_active_market(session)
                        if market_changed:
                            logger.info(
                                "Active market changed; reconnecting Polymarket WS — old_slug=%s new_slug=%s",
                                market_slug,
                                self._market_meta["market_slug"],  # type: ignore[index]
                            )
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        raw = msg.data
                        if not raw or not raw.strip():
                            continue
                        try:
                            payload = json.loads(raw)
                        except json.JSONDecodeError:
                            logger.debug("Ignoring non-JSON Polymarket message: %r", raw[:200])
                            continue
                        state = self._parse_polymarket_event(payload, latest)
                        if state is not None:
                            await queue.put(("polymarket", state))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Polymarket WS error %s: %s; reconnecting", type(exc).__name__, exc)
                await asyncio.sleep(3)

    def _parse_polymarket_event(self, payload: dict, latest: Dict[str, Tuple[Optional[float], Optional[float]]]) -> Optional[PolymarketState]:
        if self._market_meta is None:
            return None
        records = payload if isinstance(payload, list) else [payload]
        for record in records:
            asset_id = str(record.get("asset_id", ""))
            if asset_id in {self._market_meta["yes_token_id"], self._market_meta["no_token_id"]}:
                latest[asset_id] = (_best_bid(record.get("bids", [])), _best_ask(record.get("asks", [])))
        yes = latest.get(self._market_meta["yes_token_id"])
        no = latest.get(self._market_meta["no_token_id"])
        if yes is None or no is None:
            return None
        ts_raw = records[-1].get("timestamp") if records else None
        ts = _timestamp_to_datetime(ts_raw) or _utc_now()
        return PolymarketState(ts, self._market_meta["market_slug"], yes[0], yes[1], no[0], no[1])
