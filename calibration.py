"""
calibration.py — Golf model calibration from settled bets.

Reads settled rows from the `bets` table (user-placed Golf H2H + outright
bets that grade_bets.py has already marked Win/Loss/Push), buckets by
market × probability, computes actual hit rate. Lookup feeds the dashboard
display so the user sees calibrated probabilities, not raw DG output.

Same pattern as NBA + NHL — different sport, identical logic.
"""
from __future__ import annotations
import re
import json
import pandas as pd
from supabase import create_client


def _load_secrets():
    with open("golf_secrets.toml") as f:
        txt = f.read()
    url = re.search(r"SUPABASE_URL\s*=\s*['\"]([^'\"]+)", txt).group(1)
    key = re.search(r"SUPABASE_KEY\s*=\s*['\"]([^'\"]+)", txt).group(1)
    return url, key


_BUCKETS = [
    ("<55%",   0.00, 0.55),
    ("55-60%", 0.55, 0.60),
    ("60-70%", 0.60, 0.70),
    ("70-80%", 0.70, 0.80),
    ("80%+",   0.80, 1.01),
]


def _bucket(prob: float) -> str:
    for lbl, lo, hi in _BUCKETS:
        if lo <= prob < hi:
            return lbl
    return "<55%"


def _extract_dg_prob(notes: str) -> float | None:
    """Notes look like '... DG: 65.2% | Book: 60.6% ...'. Pull the DG % as a fraction."""
    m = re.search(r"DG:\s*(\d+(?:\.\d+)?)\s*%", notes or "")
    if not m:
        return None
    try:
        return float(m.group(1)) / 100.0
    except ValueError:
        return None


def load_calibration_lookup(min_n: int = 5) -> dict:
    """Returns {(market, bucket): actual_hit_rate} for buckets with ≥min_n settled."""
    url, key = _load_secrets()
    sb = create_client(url, key)
    rows = (sb.table("bets").select("*")
            .in_("result", ["Win", "Loss"])
            .execute()).data or []
    if not rows:
        return {}
    df = pd.DataFrame(rows)
    df["dg_prob"] = df["notes"].apply(_extract_dg_prob)
    df = df.dropna(subset=["dg_prob"])
    if df.empty:
        return {}
    df["bucket"] = df["dg_prob"].apply(_bucket)
    df["is_win"] = (df["result"] == "Win").astype(int)
    out = {}
    for (mkt, bucket), g in df.groupby(["market", "bucket"]):
        if len(g) < min_n:
            continue
        out[(mkt, bucket)] = float(g["is_win"].mean())
    return out


def calibrate_prob(raw_prob: float, market: str, lookup: dict) -> float:
    if raw_prob is None or raw_prob != raw_prob:
        return raw_prob
    bucket = _bucket(raw_prob)
    actual = lookup.get((market, bucket))
    if actual is None:
        return raw_prob
    return round(0.40 * raw_prob + 0.60 * actual, 4)


def summary():
    """Print current Golf calibration state."""
    lookup = load_calibration_lookup(min_n=3)  # lower threshold for sparse Golf data
    print(f"=== Golf Calibration ({len(lookup)} buckets with n≥3) ===")
    for (mkt, bucket), actual in sorted(lookup.items()):
        print(f"  {mkt:6} {bucket:8} → actual {actual*100:5.1f}%")


if __name__ == "__main__":
    summary()
