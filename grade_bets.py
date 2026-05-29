"""
grade_bets.py
Auto-grade pending Golf bets in Supabase against DataGolf historical results.

Run after a tournament concludes to settle PENDING bets:
    python3 grade_bets.py

Currently handles:
  * H2H matchups — compares 4-round totals; missed-cut counts as worst
  * Outright Win — bets table.player_name == event winner

Tournament context is parsed from the bet's `notes` field (e.g. "[Masters Tournament]").
The event_id is resolved via the schedule table by tournament name + nearest year.
"""
from __future__ import annotations
import re
import time
import requests
from typing import Optional
from supabase import create_client


# ── Secrets ───────────────────────────────────────────────────────────────────
def _load_secrets():
    with open("golf_secrets.toml") as f:
        txt = f.read()
    url = re.search(r"SUPABASE_URL\s*=\s*['\"]([^'\"]+)", txt).group(1)
    key = re.search(r"SUPABASE_KEY\s*=\s*['\"]([^'\"]+)", txt).group(1)
    return url, key


# DataGolf API key is hardcoded in golf_sync.py — re-use it
def _dg_key():
    with open("golf_sync.py") as f:
        src = f.read()
    return re.search(r"DG_API_KEY\s*=\s*['\"]([^'\"]+)", src).group(1)


SUPABASE_URL, SUPABASE_KEY = _load_secrets()
DG_KEY = _dg_key()
DG_BASE = "https://feeds.datagolf.com"

sb = create_client(SUPABASE_URL, SUPABASE_KEY)


# ── Tournament/event resolution ───────────────────────────────────────────────
def find_event(tournament_name: str, bet_date_iso: str) -> Optional[dict]:
    """Look up event_id from the schedule table by name + year (year inferred from bet date)."""
    year = int(bet_date_iso[:4])
    rows = sb.table("schedule").select("*").execute().data or []
    # Exact name match in same year, prefer that; else partial match
    cand = [r for r in rows if str(r.get("event_name", "")).strip() == tournament_name.strip() and r.get("season") == year]
    if not cand:
        cand = [r for r in rows if tournament_name.lower() in str(r.get("event_name", "")).lower() and r.get("season") == year]
    if not cand:
        return None
    return cand[0]


# ── DataGolf round fetch ──────────────────────────────────────────────────────
_round_cache: dict = {}


def fetch_event_rounds(tour: str, event_id: int | str, year: int) -> dict[str, int]:
    """Return {player_name: total_score_to_par_or_strokes}.

    Players who missed the cut are returned with their available round totals.
    Players with no scores are excluded (treated as missing).
    """
    cache_key = (tour, str(event_id), year)
    if cache_key in _round_cache:
        return _round_cache[cache_key]

    url = f"{DG_BASE}/historical-raw-data/rounds"
    params = {"tour": tour, "event_id": event_id, "year": year, "file_format": "json", "key": DG_KEY}
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code == 429:
                time.sleep(60 if attempt == 0 else 120)
                continue
            r.raise_for_status()
            data = r.json()
            break
        except requests.RequestException as e:
            if attempt == 2:
                print(f"  ! DG rounds fetch failed for event={event_id}: {e}")
                _round_cache[cache_key] = {}
                return {}
            time.sleep(5)

    totals: dict[str, int] = {}
    for p in data.get("scores", []):
        name = p.get("player_name")
        if not name:
            continue
        total = 0
        any_round = False
        for r_key in ("round_1", "round_2", "round_3", "round_4"):
            rd = p.get(r_key)
            if rd and isinstance(rd, dict) and rd.get("score") is not None:
                try:
                    total += int(rd["score"])
                    any_round = True
                except (TypeError, ValueError):
                    pass
        if any_round:
            totals[name] = total

    _round_cache[cache_key] = totals
    print(f"  · cached {len(totals)} player totals for {tour}/{event_id}/{year}")
    return totals


# ── Bet grading ───────────────────────────────────────────────────────────────
def parse_h2h(bet: dict) -> Optional[tuple[str, str, str]]:
    """Return (tournament_name, player_name, opponent_name) or None."""
    notes = bet.get("notes") or ""
    tags = re.findall(r"\[([^\]]+)\]", notes)
    # AUTO_SHADOW is our shadow-pick marker tag — never the tournament. Strip it
    # before matching. Also broadened the tournament-keyword list to cover the
    # common PGA event suffixes that were missing (Challenge, Cup, Pro-Am,
    # Schwab, FedEx, Travelers, Wells Fargo, etc.) so AUTO_SHADOW rows can
    # actually grade.
    tags = [t for t in tags if t != "AUTO_SHADOW"]
    _KW = (
        "Masters", "Open", "Championship", "Classic", "Invitational", "Tournament",
        "Heritage", "Pebble", "Texas", "Memorial", "Players", "Genesis",
        "Challenge", "Cup", "Pro-Am", "Schwab", "FedEx", "Travelers", "Wells Fargo",
        "AT&T", "Sentry", "Sony", "American Express", "Farmers", "Phoenix",
        "Honda", "Cognizant", "RBC", "Valspar", "Valero", "Arnold Palmer",
        "Mexico", "WGC", "WM", "Wyndham", "BMW", "Tour Championship",
        "Mayakoba", "Houston", "Charles Schwab",
    )
    tournament = next((t for t in tags if any(k in t for k in _KW)), None)
    if not tournament:
        return None
    # opponent: "... vs Garcia, Sergio | DG: 65.2% | Book: 60.6%"
    m = re.search(r"\bvs\s+([^|]+?)\s*\|", notes)
    if not m:
        return None
    opponent = m.group(1).strip()
    player = (bet.get("player_name") or "").strip()
    if not player:
        return None
    return tournament, player, opponent


def grade_one(bet: dict) -> Optional[str]:
    """Return 'Win' / 'Loss' / 'Push' or None if cannot be graded."""
    market = (bet.get("market") or "").upper()
    bet_date = (bet.get("logged_at") or "")[:10]
    if not bet_date:
        return None

    if market == "H2H":
        parsed = parse_h2h(bet)
        if not parsed:
            print(f"  bet {bet['id']}: could not parse tournament/opponent from notes")
            return None
        tournament, player, opponent = parsed
        evt = find_event(tournament, bet_date)
        if not evt:
            print(f"  bet {bet['id']}: event '{tournament}' not in schedule")
            return None

        tour = evt.get("tour", "pga")
        eid  = evt.get("event_id")
        year = int(evt.get("season") or bet_date[:4])
        totals = fetch_event_rounds(tour, eid, year)

        p_score = totals.get(player)
        o_score = totals.get(opponent)

        # Missed cut handling: a player not in totals never made the cut → effectively a higher score
        if p_score is None and o_score is None:
            print(f"  bet {bet['id']}: neither {player} nor {opponent} found in event totals")
            return None
        if p_score is None:
            return "Loss"
        if o_score is None:
            return "Win"
        if p_score < o_score:
            return "Win"
        if p_score > o_score:
            return "Loss"
        return "Push"

    # Outright winner
    if market in ("WIN", "OUTRIGHT", "TOURNAMENT WIN"):
        notes = bet.get("notes") or ""
        tags = re.findall(r"\[([^\]]+)\]", notes)
        tournament = next((t for t in tags if any(k in t for k in
            ("Masters", "Open", "Championship", "Classic", "Invitational",
             "Tournament", "Heritage", "Pebble", "Texas", "Memorial", "Players"))), None)
        if not tournament:
            return None
        evt = find_event(tournament, bet_date)
        if not evt:
            return None
        winner = (evt.get("winner") or "").split("(")[0].strip()
        if not winner:
            return None
        return "Win" if (bet.get("player_name") or "").strip() == winner else "Loss"

    return None


def main():
    pending = sb.table("bets").select("*").eq("result", "Pending").execute().data or []
    print(f"Pending bets: {len(pending)}")

    graded = updated = 0
    summary = {"Win": 0, "Loss": 0, "Push": 0}
    for bet in pending:
        outcome = grade_one(bet)
        if outcome is None:
            continue
        graded += 1
        summary[outcome] += 1

        # Compute profit_loss based on American odds + stake
        odds  = float(bet.get("odds") or 0)
        stake = float(bet.get("stake") or 0)
        if outcome == "Win":
            pnl = stake * (odds / 100.0) if odds > 0 else stake * (100.0 / abs(odds)) if odds < 0 else 0.0
        elif outcome == "Loss":
            pnl = -stake
        else:
            pnl = 0.0

        sb.table("bets").update({
            "result":      outcome,
            "profit_loss": round(pnl, 2),
        }).eq("id", bet["id"]).execute()
        updated += 1
        print(f"  ✓ id={bet['id']} {bet.get('player_name')} {bet.get('market')} → {outcome}  (P/L: {pnl:+.2f})")

    print(f"\nGraded {graded}/{len(pending)} | Win/Loss/Push: {summary['Win']}/{summary['Loss']}/{summary['Push']} | Updated {updated} rows")


if __name__ == "__main__":
    main()
