from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional


RTDS_TOPIC = "crypto_prices_chainlink"
BUCKET_MS = 60 * 1000


@dataclass(frozen=True)
class PriceToBeatRecord:
    symbol: str
    bucket_start: str
    bucket_start_ms: int
    price_to_beat: float
    source_timestamp: int
    received_at: str


def utc_iso_from_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def bucket_start_ms(timestamp_ms: int) -> int:
    return (timestamp_ms // BUCKET_MS) * BUCKET_MS


def normalize_symbol(symbol: str) -> str:
    symbol = symbol.strip().lower()
    if symbol.endswith("usdt") and "/" not in symbol:
        symbol = f"{symbol[:-4]}/usd"
    return symbol


def record_key(symbol: str, bucket_ms: int) -> str:
    return f"{symbol}:{bucket_ms}"


def _message_topic(message: dict[str, Any]) -> Any:
    return message.get("topic") or message.get("channel") or message.get("subscription")


def _message_type(message: dict[str, Any]) -> Any:
    return message.get("type") or message.get("event")


def _iter_candidate_messages(message: Any, inherited_rtds: bool = False) -> Iterable[tuple[dict[str, Any], bool]]:
    """Yield possible RTDS event dictionaries from common websocket wrappers.

    RTDS has changed/enveloped messages over time. In live operation we may see
    direct event dictionaries, lists of events, or wrappers such as
    ``{"data": ...}``, ``{"payload": [...]}``, etc. Keep this parser tolerant so
    the bot does not silently miss price_to_beat because of a shallow wrapper.
    """

    if isinstance(message, list):
        for item in message:
            yield from _iter_candidate_messages(item, inherited_rtds=inherited_rtds)
        return

    if not isinstance(message, dict):
        return

    payload = message.get("payload")
    topic = _message_topic(message)
    # Some RTDS responses omit topic/symbol from each child point and only carry
    # subscription context in the websocket request. Treat payload.data batches
    # as RTDS-compatible so children like {timestamp, value} can be parsed with
    # the caller-provided default symbol.
    is_rtds_context = inherited_rtds or topic == RTDS_TOPIC or (isinstance(payload, dict) and isinstance(payload.get("data"), list))

    yield message, is_rtds_context

    for key in ("data", "event", "message"):
        nested = message.get(key)
        if isinstance(nested, (dict, list)):
            yield from _iter_candidate_messages(nested, inherited_rtds=is_rtds_context)

    if isinstance(payload, list):
        yield from _iter_candidate_messages(payload, inherited_rtds=is_rtds_context)
    elif isinstance(payload, dict) and any(k in payload for k in ("data", "event", "message", "payload")):
        yield from _iter_candidate_messages(payload, inherited_rtds=is_rtds_context)


def _extract_one(message: dict[str, Any], default_symbol: Optional[str] = None, rtds_context: bool = False) -> Optional[PriceToBeatRecord]:
    topic = _message_topic(message)
    msg_type = _message_type(message)
    if topic != RTDS_TOPIC and not rtds_context:
        return None
    if topic == RTDS_TOPIC and msg_type not in {"update", "price_change", "tick", "*", None}:
        return None

    payload = message.get("payload") or message.get("data") or message
    if not isinstance(payload, dict):
        return None

    symbol = str(payload.get("symbol") or payload.get("asset") or payload.get("ticker") or default_symbol or "").lower()
    value = payload.get("value") or payload.get("price") or payload.get("answer")
    source_timestamp = payload.get("timestamp") or payload.get("ts") or payload.get("updatedAt")
    if not symbol or value is None or source_timestamp is None:
        return None
    try:
        price = float(value)
        source_timestamp_ms = int(source_timestamp)
    except (TypeError, ValueError):
        return None
    start_ms = bucket_start_ms(source_timestamp_ms)
    return PriceToBeatRecord(
        symbol=symbol,
        bucket_start=utc_iso_from_ms(start_ms),
        bucket_start_ms=start_ms,
        price_to_beat=price,
        source_timestamp=source_timestamp_ms,
        received_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    )


def extract_records(message: Any, default_symbol: Optional[str] = None) -> list[PriceToBeatRecord]:
    records: list[PriceToBeatRecord] = []
    seen: set[tuple[str, int, int, float]] = set()
    for candidate, rtds_context in _iter_candidate_messages(message):
        rec = _extract_one(candidate, default_symbol=default_symbol, rtds_context=rtds_context)
        if rec is not None:
            key = (rec.symbol, rec.bucket_start_ms, rec.source_timestamp, rec.price_to_beat)
            if key not in seen:
                records.append(rec)
                seen.add(key)
    return records


def extract_record(message: Any, default_symbol: Optional[str] = None) -> Optional[PriceToBeatRecord]:
    records = extract_records(message, default_symbol=default_symbol)
    if records:
        return records[0]
    return None


class PriceToBeatStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.records: dict[str, PriceToBeatRecord] = self._load()

    def _load(self) -> dict[str, PriceToBeatRecord]:
        if not self.path.exists():
            return {}
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        items = raw if isinstance(raw, list) else raw.get("records", [])
        records = {}
        for item in items:
            try:
                rec = PriceToBeatRecord(**item)
                records[record_key(rec.symbol, rec.bucket_start_ms)] = rec
            except TypeError:
                continue
        return records

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = [asdict(r) for r in sorted(self.records.values(), key=lambda x: (x.bucket_start_ms, x.symbol))]
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        tmp.replace(self.path)

    def add(self, record: PriceToBeatRecord) -> bool:
        key = record_key(record.symbol, record.bucket_start_ms)
        if key in self.records:
            return False
        self.records[key] = record
        self.save()
        return True

    def add_first_tick(self, record: PriceToBeatRecord) -> bool:
        return self.add(record)

    def get(self, symbol: str, bucket_ms: int) -> Optional[PriceToBeatRecord]:
        return self.records.get(record_key(normalize_symbol(symbol), bucket_ms))
