"""
calibration.py — Golf model calibration from settled bets.

Reads settled rows from the `bets` table (real + AUTO_SHADOW H2H bets that
grade_bets.py has already marked Win/Loss/Push), buckets by market ×
probability, computes actual hit rate, and blends it into DG's raw
probability before any edge/threshold decision is made.

LIVE as of 2026-07-07 (previously dead code — see git history / memory
golf_calibration_wired.md for the full story). `calibrate_prob()` is now
called from BOTH real decision paths: golf_sync.py's shadow_log_matchups()
(gates which matchups get logged as candidate picks) and picks_today.py's
matchup_edges() (the shared function must_picks_feed.py uses to build the
Outlet's actual Golf picks). Both pass market="H2H" literally, matching what
`bets.market` has always actually stored (confirmed: 100% of 366 rows) — the
previous docstring's fear of a "round_matchups"/"tournament_matchups"
mismatch was conflating `matchup_odds.market` (a DIFFERENT table, DataGolf's
own naming, used only by picks_today.py's --market CLI filter) with
`bets.market` (always "H2H"); there was never a real vocabulary collision at
the actual call sites, just an unwired function.

DEFAULT_MIN_N=15 for live gating (checked against real data 2026-07-07: at
this threshold only 3 buckets currently qualify, and DG's raw probabilities
are already close to identity in all three — consistent with the
independent full diagnostic run the same week finding no bucket clears
|z|>1.2. Wiring this in is about closing the loop for when/if that drifts,
not an expectation of a big edge shift today.
"""
from __future__ import annotations
import re
import json
import pandas as pd
from supabase import create_client

from golf_db import fetch_all


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

# Live-gating threshold: higher than the "3" the loose CLI summary() uses
# (that's a display-only peek), lower than F5's 20 / WC's 30 — Golf simply
# doesn't have those models' bet volume yet. Chosen so a bucket needs real
# repeated evidence before it's allowed to move a real edge decision.
DEFAULT_MIN_N = 15


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


def load_calibration_lookup(min_n: int = DEFAULT_MIN_N) -> dict:
    """Returns {(market, bucket): actual_hit_rate} for buckets with ≥min_n settled."""
    url, key = _load_secrets()
    sb = create_client(url, key)
    rows = fetch_all(lambda: sb.table("bets").select("*").in_("result", ["Win", "Loss"]))
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
    """Blend DG's raw probability toward this bucket's REALIZED hit rate
    (40% raw / 60% empirical — same shrink-toward-observed pattern F5 and WC
    use, just a different split since Golf's sample is much thinner). Callers
    MUST pass market="H2H" (the only value `bets.market` has ever actually
    stored, verified 2026-07-07) -- passing anything else guarantees a lookup
    miss and a silent no-op fallback to raw_prob below, exactly the bug this
    module sat in for weeks. No bucket match (thin data or a genuinely
    well-calibrated band) -> honest passthrough, not a forced adjustment.
    """
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
