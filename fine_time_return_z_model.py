from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


TIME_BUCKETS_FINE = [i / 10 for i in range(11)]
Z_BUCKETS = [-math.inf, -8.0, -4.0, -2.0, -1.0, 0.0, 1.0, 2.0, 4.0, 8.0, math.inf]


def time_bucket_label(elapsed_fraction: float) -> Optional[str]:
    if elapsed_fraction is None or not math.isfinite(elapsed_fraction):
        return None
    x = min(max(elapsed_fraction, 0.0), 1.0)
    for left, right in zip(TIME_BUCKETS_FINE[:-1], TIME_BUCKETS_FINE[1:]):
        if left <= x <= right if right == 1.0 else left <= x < right:
            return f"{left:.1%}..{right:.1%}"
    return "90.0%..100.0%"


def z_bucket_label(value: float) -> Optional[str]:
    if value is None or not math.isfinite(value):
        return None
    for left, right in zip(Z_BUCKETS[:-1], Z_BUCKETS[1:]):
        if math.isinf(left) and left < 0 and value <= right:
            return f"<= {right:g}"
        if math.isinf(right) and right > 0 and value > left:
            return f"> {left:g}"
        if left <= value <= right if right == math.inf else left <= value < right:
            return f"{left:g}..{right:g}"
    return None


@dataclass(frozen=True)
class LookupResult:
    fair_up_prob: float
    fallback_level: int
    keys: str
    count: Optional[float]
    time_bucket_fine: str
    return_z_bucket: str
    return_z: float


class FineTimeReturnZModel:
    def __init__(self, model_dir: Path):
        self.model_dir = Path(model_dir)
        metadata_path = self.model_dir / "fine_time_return_z_metadata.json"
        self.metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        self.global_prior = float(self.metadata.get("global_prior", 0.5))
        self.level1 = self._load_table("fine_time_return_z_lookup_level_1.csv", ["time_bucket_fine", "return_z_bucket"])
        self.level2 = self._load_table("fine_time_return_z_lookup_level_2.csv", ["return_z_bucket"])
        self.level3 = self._load_table("fine_time_return_z_lookup_level_3.csv", ["time_bucket_fine"])

    def _load_table(self, filename: str, keys: list[str]) -> dict[tuple[str, ...], tuple[float, float]]:
        table: dict[tuple[str, ...], tuple[float, float]] = {}
        with (self.model_dir / filename).open("r", encoding="utf-8", newline="") as file:
            for row in csv.DictReader(file):
                key = tuple(str(row[k]) for k in keys)
                table[key] = (float(row["fair_up_prob"]), float(row["count"]))
        return table

    def predict(self, elapsed_fraction: float, underlying_return_since_start: float, vol_60m: float) -> LookupResult:
        if vol_60m is None or not math.isfinite(vol_60m) or vol_60m <= 0:
            raise ValueError("vol_60m must be positive and finite")
        return_z = underlying_return_since_start / vol_60m
        tb = time_bucket_label(elapsed_fraction)
        zb = z_bucket_label(return_z)
        if tb is None or zb is None:
            raise ValueError("could not bucket elapsed_fraction/return_z")

        hit = self.level1.get((tb, zb))
        if hit is not None:
            return LookupResult(hit[0], 1, "time_bucket_fine+return_z_bucket", hit[1], tb, zb, return_z)
        hit = self.level2.get((zb,))
        if hit is not None:
            return LookupResult(hit[0], 2, "return_z_bucket", hit[1], tb, zb, return_z)
        hit = self.level3.get((tb,))
        if hit is not None:
            return LookupResult(hit[0], 3, "time_bucket_fine", hit[1], tb, zb, return_z)
        return LookupResult(self.global_prior, 0, "global_prior", None, tb, zb, return_z)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-dir", default="polymarketBot2/model_artifacts")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    model = FineTimeReturnZModel(Path(args.model_dir))
    if args.self_test:
        result = model.predict(elapsed_fraction=0.25, underlying_return_since_start=0.001, vol_60m=0.001)
        print(json.dumps(result.__dict__, sort_keys=True))


if __name__ == "__main__":
    main()
