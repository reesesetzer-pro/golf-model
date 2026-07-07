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
from supabase import create_client, ClientOptions

from golf_db import fetch_all


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

# 60s postgrest timeout (default was too short for a slow Supabase) + the schedule
# cache below stop golf-grade's ReadTimeout: find_event was re-fetching the FULL
# schedule table once per pending bet (~280x/run). Now fetched once.
sb = create_client(SUPABASE_URL, SUPABASE_KEY,
                   options=ClientOptions(postgrest_client_timeout=60))


# ── Tournament/event resolution ───────────────────────────────────────────────
_SCHEDULE_CACHE = None


def _schedule() -> list:
    """The schedule table, fetched ONCE per run (was re-fetched on every find_event call,
    i.e. per pending bet — the ReadTimeout culprit)."""
    global _SCHEDULE_CACHE
    if _SCHEDULE_CACHE is None:
        _SCHEDULE_CACHE = sb.table("schedule").select("*").execute().data or []
    return _SCHEDULE_CACHE


def find_event(tournament_name: str, bet_date_iso: str) -> Optional[dict]:
    """Look up event_id from the schedule table by name + year (year inferred from bet date)."""
    year = int(bet_date_iso[:4])
    rows = _schedule()
    # Exact name match in same year, prefer that; else partial match
    cand = [r for r in rows if str(r.get("event_name", "")).strip() == tournament_name.strip() and r.get("season") == year]
    if not cand:
        cand = [r for r in rows if tournament_name.lower() in str(r.get("event_name", "")).lower() and r.get("season") == year]
    if not cand:
        return None
    return cand[0]


# ── DataGolf round fetch ──────────────────────────────────────────────────────
_round_cache: dict = {}


def fetch_event_rounds(tour: str, event_id: int | str, year: int) -> dict[str, dict]:
    """Return {player_name: {"total": int, "r1": int|None, "r2": ..., "r3": ..., "r4": ...}}.

    Players who missed the cut are returned with their available round totals.
    Players with no scores are excluded (treated as missing).

    BUGFIX 2026-05-31: previously returned only summed totals. Round-specific
    H2H matchups (e.g. R2 of Charles Schwab Challenge) need per-round scores
    so we compare R2 strokes head-to-head, not R1+R2 totals. Now returns the
    per-round breakdown alongside the cumulative total.
    """
    cache_key = (tour, str(event_id), year)
    if cache_key in _round_cache:
        return _round_cache[cache_key]

    url = f"{DG_BASE}/historical-raw-data/rounds"
    params = {"tour": tour, "event_id": event_id, "year": year, "file_format": "json", "key": DG_KEY}
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=20)
            # DataGolf's historical archive only lists an event AFTER it concludes
            # (verified 2026-07-05: the exact same event_id/params 200s fine for
            # older, completed events -- this 400 is specifically "event number
            # {N} is not available in the {year} {tour} calendar year"). An
            # in-progress tournament (schedule row has winner='TBD'/end_date=None)
            # will deterministically 400 here until it wraps and DataGolf backfills
            # it -- NOT a failure, and retrying won't change the outcome, so fail
            # fast instead of burning the retry loop and printing an alarming
            # "fetch failed" that looks identical to a real, unresolvable bug.
            if r.status_code == 400 and "not available" in r.text.lower():
                print(f"  · event {event_id} not yet in DataGolf's historical archive "
                      f"(tournament likely still in progress) — bets stay pending "
                      f"until it concludes and DataGolf backfills it")
                _round_cache[cache_key] = {}
                return {}
            if r.status_code == 429:
                if attempt == 2:
                    # Exhausted retries on repeated rate-limiting: without this,
                    # the loop ends without ever assigning `data`, and the code
                    # below crashes with UnboundLocalError instead of leaving
                    # the event pending like every other failure path does.
                    print(f"  ! DG rounds fetch failed for event={event_id}: rate limited (429) after 3 attempts")
                    _round_cache[cache_key] = {}
                    return {}
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

    scores: dict[str, dict] = {}
    for p in data.get("scores", []):
        name = p.get("player_name")
        if not name:
            continue
        per_round = {}
        total = 0
        any_round = False
        for ridx, r_key in enumerate(("round_1", "round_2", "round_3", "round_4"), start=1):
            rd = p.get(r_key)
            if rd and isinstance(rd, dict) and rd.get("score") is not None:
                try:
                    s = int(rd["score"])
                    per_round[f"r{ridx}"] = s
                    total += s
                    any_round = True
                except (TypeError, ValueError):
                    per_round[f"r{ridx}"] = None
            else:
                per_round[f"r{ridx}"] = None
        if any_round:
            per_round["total"] = total
            # Keyed lowercase: DataGolf's own casing is inconsistent across
            # players (confirmed 2026-07-05 -- "van Rooyen, Erik" here vs
            # "Van Rooyen, Erik" in the odds feed / bets.player_name), and
            # grade_one()'s lookup was an exact-match dict.get with no
            # case-folding, so a casing mismatch silently fell into the
            # "opponent not found -> auto-Win the other side" branch. Confirmed
            # 2 real bets mis-graded this way (453/512: a 74-74 tie graded Win
            # instead of Push). grade_one() lowercases its lookup keys to match.
            scores[name.lower()] = per_round

    _round_cache[cache_key] = scores
    print(f"  · cached {len(scores)} player rounds for {tour}/{event_id}/{year}")
    return scores


# ── Bet grading ───────────────────────────────────────────────────────────────
# Shared by parse_h2h AND the outright-Win block below — they parse the same
# "[Tournament Name]" tag out of `notes` and both need to recognize real PGA
# event names. These used to be two separately-maintained lists; the outright
# one was a stale, much shorter copy (missing Sentry/American Express/Charles
# Schwab/FedEx/RBC/Wyndham/BMW/etc.), so outright Win bets on those events
# never matched and stayed Pending forever. One list now, used by both.
_TOURNAMENT_KW = (
    "Masters", "Open", "Championship", "Classic", "Invitational", "Tournament",
    "Heritage", "Pebble", "Texas", "Memorial", "Players", "Genesis",
    "Challenge", "Cup", "Pro-Am", "Schwab", "FedEx", "Travelers", "Wells Fargo",
    "AT&T", "Sentry", "Sony", "American Express", "Farmers", "Phoenix",
    "Honda", "Cognizant", "RBC", "Valspar", "Valero", "Arnold Palmer",
    "Mexico", "WGC", "WM", "Wyndham", "BMW", "Tour Championship",
    "Mayakoba", "Houston", "Charles Schwab",
)


def parse_h2h(bet: dict) -> Optional[tuple[str, str, str]]:
    """Return (tournament_name, player_name, opponent_name) or None."""
    notes = bet.get("notes") or ""
    tags = re.findall(r"\[([^\]]+)\]", notes)
    # AUTO_SHADOW is our shadow-pick marker tag — never the tournament. Strip it
    # before matching.
    tags = [t for t in tags if t != "AUTO_SHADOW"]
    tournament = next((t for t in tags if any(k in t for k in _TOURNAMENT_KW)), None)
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
        scores = fetch_event_rounds(tour, eid, year)

        # Determine which score to compare:
        #   - Round-specific matchup (bet.round = "R1"/"R2"/"R3"/"R4") → that
        #     round's score only (e.g. R2 H2H bets compare round_2 strokes,
        #     not cumulative R1+R2 totals).
        #   - Tournament matchup / no round info / "Live" → full event total.
        # This fixes the bug where round_matchups H2H bets graded against
        # cumulative totals, not the actual round they were bet on.
        round_field = str(bet.get("round") or "").strip().lower()
        score_key = "total"
        if round_field in ("r1", "round_1"): score_key = "r1"
        elif round_field in ("r2", "round_2"): score_key = "r2"
        elif round_field in ("r3", "round_3"): score_key = "r3"
        elif round_field in ("r4", "round_4"): score_key = "r4"

        p_record = scores.get(player.lower()) or {}
        o_record = scores.get(opponent.lower()) or {}
        p_score = p_record.get(score_key)
        o_score = o_record.get(score_key)

        # Missed cut handling: a player not in totals never made the cut → effectively a higher score
        if p_score is None and o_score is None:
            # For round-specific bets, also try total as fallback (helps when
            # the requested round hasn't been played yet — return None to
            # leave Pending rather than mis-grading).
            if score_key != "total" and not p_record and not o_record:
                # Only worth a per-bet line when `scores` actually came back with
                # SOME data (a real, specific name-match miss). When the whole
                # event's fetch came back empty (event not archived yet — see
                # fetch_event_rounds' one-time message above), every bet in the
                # event hits this same branch for the same reason; repeating it
                # 50+ times is pure noise that buries a genuine mismatch the next
                # time this fires for a completed event.
                if scores:
                    print(f"  bet {bet['id']}: neither {player} nor {opponent} in event totals")
                return None
            print(f"  bet {bet['id']}: no {score_key} score for {player} or {opponent}")
            return None
        if p_score is None:
            return "Loss"
        if o_score is None:
            return "Win"
        # Tournament-total comparisons: a missed-cut (or WD) player's `total`
        # is only a PARTIAL score (e.g. 2 rounds), which can be numerically
        # LOWER than a made-cut player's full 4-round total even though
        # missing the cut should always be the worse outcome ("missed-cut
        # counts as worst", per this file's own docstring) — comparing the
        # raw totals directly let a missed-cut player win on paper. Detect an
        # incomplete tournament via missing r3/r4 and force that side to Loss
        # before falling back to a raw total comparison (which still applies
        # correctly when BOTH players missed the cut, comparing like-for-like
        # partial totals through the same number of rounds).
        if score_key == "total":
            p_made_cut = p_record.get("r3") is not None and p_record.get("r4") is not None
            o_made_cut = o_record.get("r3") is not None and o_record.get("r4") is not None
            if p_made_cut != o_made_cut:
                return "Win" if p_made_cut else "Loss"
            # Same made_cut status (both False here) doesn't mean the same ROUND
            # COUNT: a player who withdrew AFTER making the 36-hole cut (3 rounds
            # played, e.g. r1/r2/r3 present) looks identical to a genuine 36-hole
            # miss (2 rounds) under the r3/r4-presence check above -- both have
            # r4=None. Their raw `total` then isn't apples-to-apples (a 3-round
            # total vs a 2-round total). Verified live: 2026 RBC Canadian Open has
            # both cases in the same field (Koepka: WD after 3 rounds, 204 total;
            # several genuine 36-hole misses at 2 rounds, ~139 total) -- comparing
            # those totals directly can invert the "missed cut = worst" rule
            # depending on the specific scores. Detect via round-count parity
            # instead of guessing; leave pending rather than compare unlike things.
            p_rounds = sum(1 for k in ("r1", "r2", "r3", "r4") if p_record.get(k) is not None)
            o_rounds = sum(1 for k in ("r1", "r2", "r3", "r4") if o_record.get(k) is not None)
            if p_rounds != o_rounds:
                print(f"  bet {bet['id']}: {player} ({p_rounds}rd) vs {opponent} ({o_rounds}rd) "
                      f"— mismatched round counts (withdrew-after-cut vs missed-cut?), "
                      f"can't compare like-for-like, leaving pending")
                return None
        if p_score < o_score:
            return "Win"
        if p_score > o_score:
            return "Loss"
        return "Push"

    # Outright winner
    if market in ("WIN", "OUTRIGHT", "TOURNAMENT WIN"):
        notes = bet.get("notes") or ""
        tags = re.findall(r"\[([^\]]+)\]", notes)
        tournament = next((t for t in tags if any(k in t for k in _TOURNAMENT_KW)), None)
        if not tournament:
            return None
        evt = find_event(tournament, bet_date)
        if not evt:
            return None
        # winner can be MULTIPLE co-champions in a team event, ";"-separated
        # (e.g. "Fitzpatrick, Alex (25362);Fitzpatrick, Matt (17646)" for the
        # 2026 Zurich Classic) -- splitting once on "(" before also splitting
        # on ";" silently kept only the first name, so a Win bet on the
        # second-listed co-champion always graded Loss even though he won.
        # Case-insensitive compare added to match the H2H branch's existing
        # .lower() fix (same DataGolf-vs-feed casing risk applies here too;
        # dormant today only because 0/366 live bets use this market).
        winners = [w.split("(")[0].strip().lower()
                   for w in (evt.get("winner") or "").split(";") if w.strip()]
        if not winners:
            return None
        return "Win" if (bet.get("player_name") or "").strip().lower() in winners else "Loss"

    return None


def _closing_clv(bet: dict) -> Optional[float]:
    """CLV% for a shadow bet vs the last pre-round odds snapshot that
    golf_sync.py writes to closing_lines.json.

    FIX (2026-07-07): a bet's own SHADOW_KEY embeds "event|round|player|
    opponent" in WHICHEVER order the bet was logged (the shadow-logged side
    can be either p1 or p2), but closing_lines.json is always snapshotted
    canonically as "event|round|p1_name|p2_name" regardless. For a bet logged
    on the p2 side, the two key orderings never match, so the old code (which
    always looked up `key` as-is and always read the "p1" field) silently
    returned None even when a real snapshot existed under the reversed key.
    Confirmed live: 22 of 55 bets with an available snapshot (40%) were
    dropped this way, silently biasing the avg-CLV line this file's own
    main() prints. Fix: try both orderings, and read whichever side of the
    record the FOUND key's ordering says is this bet's own player -- not
    hardcoded "p1"."""
    import json as _json, os as _os
    notes = bet.get("notes") or ""
    if "SHADOW_KEY=" not in notes:
        return None
    key = notes.split("SHADOW_KEY=", 1)[1].strip()
    parts = key.split("|")
    if len(parts) != 4:
        return None
    event, rnd, player, opponent = parts
    try:
        path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "closing_lines.json")
        lines = _json.load(open(path))
        rec, side = lines.get(key), "p1"
        if rec is None:
            rec, side = lines.get(f"{event}|{rnd}|{opponent}|{player}"), "p2"
        if rec is None:
            return None
        close = rec.get(side)
        bet_odds = float(bet.get("odds") or 0)
        close = float(close)
        if not bet_odds or not close:
            return None
        dec = lambda a: 1 + (a / 100 if a > 0 else 100 / abs(a))
        return (dec(bet_odds) / dec(close) - 1) * 100
    except Exception:
        return None


def main():
    pending = fetch_all(lambda: sb.table("bets").select("*").eq("result", "Pending"))
    print(f"Pending bets: {len(pending)}")

    graded = updated = 0
    summary = {"Win": 0, "Loss": 0, "Push": 0}
    clvs = []
    for bet in pending:
        outcome = grade_one(bet)
        if outcome is None:
            continue
        graded += 1
        summary[outcome] += 1
        clv = _closing_clv(bet)
        if clv is not None:
            clvs.append(clv)

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

    clv_note = (f" | avg CLV {sum(clvs)/len(clvs):+.2f}% (n={len(clvs)})"
                if clvs else "")
    print(f"\nGraded {graded}/{len(pending)} | Win/Loss/Push: {summary['Win']}/{summary['Loss']}/{summary['Push']} | Updated {updated} rows{clv_note}")


if __name__ == "__main__":
    main()
