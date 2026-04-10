"""
golf_sync.py — Golf H2H Betting Model Data Sync
Pulls DataGolf API + The Odds API → Supabase

Tables managed:
  players            — DataGolf player list + IDs
  schedule           — PGA Tour season schedule
  field              — Current week field + tee times
  skill_ratings      — SG component ratings per player
  rounds             — Historical round-level scores + SG splits
  predictions        — Pre-tournament model win/top5/top10/cut probs
  live_predictions   — In-tournament updated probabilities
  matchup_odds       — DataGolf model H2H matchup odds
  historical_odds    — Historical opening + closing lines (matchups)
  book_odds          — Live lines from 8 books via The Odds API

Usage:
  python golf_sync.py                  # Full sync
  python golf_sync.py --mode live      # Live/in-tournament only (faster)
  python golf_sync.py --mode pre       # Pre-tournament predictions only
  python golf_sync.py --mode setup     # Print SQL to create Supabase tables
"""

import os
import sys
import json
import time
import logging
import argparse
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ── Credentials ────────────────────────────────────────────────────────────────
DG_API_KEY   = "c896625a505f2b6b6334c7495883"
SUPABASE_URL = "https://ithrwemqnmgrjvutqmhl.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Iml0aHJ3ZW1xbm1ncmp2dXRxbWhsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU2MDkxNDMsImV4cCI6MjA5MTE4NTE0M30.d9Cm7PiVrB566JJYRQUPZJcWiV5v8wlvQzaHh-NIJkE"
ODDS_API_KEY = "40cfbba84e52cd6da31272d4ac287966"

# ── DataGolf Base URL ───────────────────────────────────────────────────────────
DG_BASE = "https://feeds.datagolf.com"

# ── The Odds API config ─────────────────────────────────────────────────────────
# NOTE: Odds API covers golf OUTRIGHTS only (no round H2H matchups).
# Round H2H lines come from DataGolf's betting-tools/matchups endpoint instead.
ODDS_BASE    = "https://api.the-odds-api.com/v4"
ODDS_REGION  = "us2"
ODDS_MARKETS = "outrights"
ODDS_BOOKS   = [
    "draftkings", "fanduel", "hardrock",
    "thescore", "betmgm", "caesars", "bet365"
]
# All golf outright markets available on The Odds API
GOLF_SPORTS = [
    "golf_masters_tournament_winner",
    "golf_pga_championship_winner",
    "golf_the_open_championship_winner",
    "golf_us_open_winner",
]

# The Odds API sport key → list of (odds_api_market, finish_odds_market) tuples
# Each major has separate sport keys per finish market
GOLF_FINISH_MARKET_MAP = {
    "golf_masters_tournament_winner":       "win",
    "golf_masters_tournament_make_cut":     "make_cut",
    "golf_masters_tournament_top_5":        "top_5",
    "golf_masters_tournament_top_10":       "top_10",
    "golf_masters_tournament_top_20":       "top_20",
    "golf_pga_championship_winner":         "win",
    "golf_pga_championship_make_cut":       "make_cut",
    "golf_pga_championship_top_5":          "top_5",
    "golf_pga_championship_top_10":         "top_10",
    "golf_pga_championship_top_20":         "top_20",
    "golf_the_open_championship_winner":    "win",
    "golf_the_open_championship_make_cut":  "make_cut",
    "golf_the_open_championship_top_5":     "top_5",
    "golf_the_open_championship_top_10":    "top_10",
    "golf_the_open_championship_top_20":    "top_20",
    "golf_us_open_winner":                  "win",
    "golf_us_open_make_cut":                "make_cut",
    "golf_us_open_top_5":                   "top_5",
    "golf_us_open_top_10":                  "top_10",
    "golf_us_open_top_20":                  "top_20",
}

# ── Logging ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("golf_sync")

# ── Supabase client ──────────────────────────────────────────────────────────────
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def dg_get(endpoint: str, params: dict = {}) -> dict | list | None:
    """GET a DataGolf endpoint with error handling and rate-limit backoff."""
    params["key"] = DG_API_KEY
    params.setdefault("file_format", "json")
    url = f"{DG_BASE}/{endpoint}"
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code == 429:
                wait = 60 if attempt == 0 else 120
                log.warning(f"Rate limited — waiting {wait}s before retry {attempt+1}")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            log.error(f"DG request failed ({endpoint}): {e}")
            if attempt < 2:
                time.sleep(5)
    return None


def odds_get(endpoint: str, params: dict = {}) -> dict | list | None:
    """GET a The Odds API endpoint."""
    params["apiKey"] = ODDS_API_KEY
    url = f"{ODDS_BASE}/{endpoint}"
    try:
        r = requests.get(url, params=params, timeout=20)
        remaining = r.headers.get("x-requests-remaining", "?")
        log.info(f"Odds API credits remaining: {remaining}")
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        log.error(f"Odds API request failed ({endpoint}): {e}")
        return None


def upsert(table: str, rows: list[dict], conflict_col: str = None) -> bool:
    """Upsert rows into a Supabase table."""
    if not rows:
        log.warning(f"No rows to upsert into {table}")
        return False
    try:
        kwargs = {}
        if conflict_col:
            kwargs["on_conflict"] = conflict_col
        supabase.table(table).upsert(rows, **kwargs).execute()
        log.info(f"✓  {table:25s} — {len(rows)} rows upserted")
        return True
    except Exception as e:
        log.error(f"Supabase upsert failed ({table}): {e}")
        return False


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


# ═══════════════════════════════════════════════════════════════════════════════
# TABLE SETUP  (run once with --mode setup)
# ═══════════════════════════════════════════════════════════════════════════════

SUPABASE_SQL = """
-- Players
create table if not exists players (
    dg_id           integer primary key,
    player_name     text,
    country         text,
    amateur         boolean,
    updated_at      timestamptz default now()
);

-- Schedule
create table if not exists schedule (
    event_id        text primary key,
    event_name      text,
    tour            text,
    season          integer,
    course          text,
    location        text,
    start_date      date,
    end_date        date,
    winner          text,
    updated_at      timestamptz default now()
);

-- Field (current week)
create table if not exists field (
    dg_id           integer,
    event_id        text,
    player_name     text,
    country         text,
    amateur         boolean,
    r1_teetime      text,
    r2_teetime      text,
    start_hole      integer,
    withdrawn       boolean default false,
    updated_at      timestamptz default now(),
    primary key (dg_id, event_id)
);

-- Skill ratings
create table if not exists skill_ratings (
    dg_id           integer primary key,
    player_name     text,
    dg_rank         integer,
    sg_total        numeric,
    sg_ott          numeric,
    sg_app          numeric,
    sg_atg          numeric,
    sg_putt         numeric,
    driving_dist    numeric,
    driving_acc     numeric,
    updated_at      timestamptz default now()
);

-- Historical rounds
create table if not exists rounds (
    id              bigserial primary key,
    dg_id           integer,
    player_name     text,
    event_id        text,
    event_name      text,
    year            integer,
    tour            text,
    round_num       integer,
    course          text,
    score           integer,
    sg_total        numeric,
    sg_ott          numeric,
    sg_app          numeric,
    sg_atg          numeric,
    sg_putt         numeric,
    tee_time        text,
    group_id        text,
    updated_at      timestamptz default now(),
    unique (dg_id, event_id, year, round_num)
);

-- Pre-tournament predictions
create table if not exists predictions (
    dg_id           integer,
    event_id        text,
    player_name     text,
    tour            text,
    baseline_win    numeric,
    baseline_top5   numeric,
    baseline_top10  numeric,
    baseline_top20  numeric,
    baseline_make_cut numeric,
    course_win      numeric,
    course_top5     numeric,
    course_top10    numeric,
    course_top20    numeric,
    course_make_cut numeric,
    updated_at      timestamptz default now(),
    primary key (dg_id, event_id)
);

-- Live (in-tournament) predictions
create table if not exists live_predictions (
    dg_id           integer,
    event_id        text,
    player_name     text,
    current_pos     integer,
    current_score   integer,
    thru            integer,
    win_prob        numeric,
    top5_prob       numeric,
    top10_prob      numeric,
    make_cut_prob   numeric,
    updated_at      timestamptz default now(),
    primary key (dg_id, event_id)
);

-- DataGolf matchup odds
create table if not exists matchup_odds (
    id              bigserial primary key,
    event_id        text,
    market          text,
    round_num       integer,
    p1_dg_id        integer,
    p1_name         text,
    p2_dg_id        integer,
    p2_name         text,
    p1_dg_win_prob  numeric,
    p2_dg_win_prob  numeric,
    tie_prob        numeric,
    p1_dg_odds      integer,
    p2_dg_odds      integer,
    updated_at      timestamptz default now(),
    unique (event_id, market, round_num, p1_dg_id, p2_dg_id)
);

-- Historical matchup odds (opening + closing lines)
create table if not exists historical_odds (
    id              bigserial primary key,
    event_id        text,
    year            integer,
    market          text,
    book            text,
    p1_name         text,
    p2_name         text,
    p1_open_odds    integer,
    p2_open_odds    integer,
    p1_close_odds   integer,
    p2_close_odds   integer,
    updated_at      timestamptz default now(),
    unique (event_id, year, market, book, p1_name, p2_name)
);

-- Finish position odds (Win / Top5 / Top10 / Top20 / Make Cut)
create table if not exists finish_odds (
    dg_id           integer,
    event_id        text,
    market          text,
    player_name     text,
    tour            text,
    dg_prob         numeric,
    dg_odds         integer,
    draftkings      integer,
    fanduel         integer,
    betmgm          integer,
    caesars         integer,
    bet365          integer,
    thescore        integer,
    hardrock        integer,
    best_odds       integer,
    best_book       text,
    updated_at      timestamptz default now(),
    primary key (dg_id, event_id, market)
);

-- Live book odds (from The Odds API)
create table if not exists book_odds (
    id              bigserial primary key,
    event_id        text,
    market          text,
    bookmaker       text,
    p1_name         text,
    p2_name         text,
    p1_odds         integer,
    p2_odds         integer,
    last_update     timestamptz,
    pulled_at       timestamptz default now(),
    unique (event_id, market, bookmaker, p1_name, p2_name)
);

-- Bet results tracker
create table if not exists bets (
    id              bigserial primary key,
    player_name     text,
    market          text,
    side            text,
    book            text,
    odds            integer,
    stake           numeric,
    to_win          numeric,
    implied_prob    numeric,
    edge_at_bet     numeric,
    round           text,
    notes           text,
    result          text default 'Pending',
    profit_loss     numeric default 0,
    logged_at       timestamptz default now()
);
"""

def setup_tables():
    """Print SQL to run in Supabase SQL editor to create all tables."""
    log.info("Copy and run the following SQL in your Supabase SQL editor:")
    print("\n" + "="*70)
    print(SUPABASE_SQL)
    print("="*70 + "\n")


# ═══════════════════════════════════════════════════════════════════════════════
# SYNC FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def sync_players():
    log.info("Syncing players...")
    data = dg_get("get-player-list")
    if not data:
        return
    rows = [
        {
            "dg_id":       p.get("dg_id"),
            "player_name": p.get("player_name"),
            "country":     p.get("country"),
            "amateur":     bool(p.get("amateur", False)),
            "updated_at":  now_utc(),
        }
        for p in data
        if p.get("dg_id")
    ]
    upsert("players", rows, "dg_id")


def sync_schedule(tour: str = "pga", season: int = 2026):
    log.info(f"Syncing schedule ({tour} {season})...")
    data = dg_get("get-schedule", {"tour": tour, "season": season})
    if not data:
        return
    events = data if isinstance(data, list) else data.get("schedule", [])
    rows = [
        {
            "event_id":   str(e.get("event_id", "")),
            "event_name": e.get("event_name"),
            "tour":       tour,
            "season":     season,
            "course":     e.get("course"),
            "location":   e.get("location"),
            "start_date": e.get("date"),
            "winner":     e.get("winner"),
            "updated_at": now_utc(),
        }
        for e in events
        if e.get("event_id")
    ]
    upsert("schedule", rows, "event_id")


def sync_field(tour: str = "pga"):
    log.info(f"Syncing field ({tour})...")
    data = dg_get("field-updates", {"tour": tour})
    if not data:
        return
    event_id = str(data.get("event_id", "current"))
    field    = data.get("field", [])
    rows = [
        {
            "dg_id":       p.get("dg_id"),
            "event_id":    event_id,
            "player_name": p.get("player_name"),
            "country":     p.get("country"),
            "amateur":     bool(p.get("amateur", False)),
            "r1_teetime":  p.get("r1_teetime"),
            "r2_teetime":  p.get("r2_teetime"),
            "start_hole":  p.get("start_hole"),
            "withdrawn":   bool(p.get("wd", False)),
            "updated_at":  now_utc(),
        }
        for p in field
        if p.get("dg_id")
    ]
    upsert("field", rows, "dg_id,event_id")


def sync_skill_ratings():
    log.info("Syncing skill ratings...")
    data = dg_get("preds/skill-ratings", {"display": "value"})
    if not data:
        return
    players = data.get("players", data) if isinstance(data, dict) else data
    # Sort by sg_total descending to derive rank
    players_sorted = sorted(
        [p for p in players if p.get("sg_total") is not None],
        key=lambda x: x.get("sg_total", 0),
        reverse=True
    )
    rows = []
    for rank, p in enumerate(players_sorted, 1):
        rows.append({
            "dg_id":        p.get("dg_id"),
            "player_name":  p.get("player_name"),
            "dg_rank":      rank,
            "sg_total":     p.get("sg_total"),
            "sg_ott":       p.get("sg_ott"),
            "sg_app":       p.get("sg_app"),
            "sg_atg":       p.get("sg_arg"),   # DataGolf returns sg_arg
            "sg_putt":      p.get("sg_putt"),
            "driving_dist": p.get("driving_dist"),
            "driving_acc":  p.get("driving_acc"),
            "updated_at":   now_utc(),
        })
    upsert("skill_ratings", rows, "dg_id")


def sync_rounds(tour: str = "pga", year: int = 2026, event_id: str = "all"):
    """Pull historical round data. DataGolf returns scores nested per player
    with round_1/round_2/etc as sub-keys. We flatten to one row per round."""
    log.info(f"Syncing rounds ({tour} {year} event={event_id})...")

    # If event_id='all', get the full schedule and loop each event
    if event_id == "all":
        sched = dg_get("get-schedule", {"tour": tour, "season": year})
        if not sched:
            return
        events = sched if isinstance(sched, list) else sched.get("schedule", [])
        event_ids = [str(e.get("event_id")) for e in events if e.get("event_id")]
    else:
        event_ids = [str(event_id)]

    all_rows = []
    for eid in event_ids:
        data = dg_get("historical-raw-data/rounds", {
            "tour":     tour,
            "event_id": eid,
            "year":     year,
        })
        if not data or not isinstance(data, dict):
            continue

        event_name = data.get("event_name", "")
        scores     = data.get("scores", [])

        for player in scores:
            dg_id       = player.get("dg_id")
            player_name = player.get("player_name")

            # Each round is stored as round_1, round_2, round_3, round_4
            for rnum in [1, 2, 3, 4]:
                rkey = f"round_{rnum}"
                r = player.get(rkey)
                if not r:
                    continue
                all_rows.append({
                    "dg_id":       dg_id,
                    "player_name": player_name,
                    "event_id":    eid,
                    "event_name":  event_name,
                    "year":        year,
                    "tour":        tour,
                    "round_num":   rnum,
                    "course":      r.get("course_name"),
                    "score":       r.get("score"),
                    "sg_total":    r.get("sg_total"),
                    "sg_ott":      r.get("sg_ott"),
                    "sg_app":      r.get("sg_app"),
                    "sg_atg":      r.get("sg_arg"),
                    "sg_putt":     r.get("sg_putt"),
                    "tee_time":    r.get("teetime"),
                    "group_id":    str(r.get("group_id", "")),
                    "updated_at":  now_utc(),
                })

        # Batch upsert every 5 events to avoid huge payloads
        if len(all_rows) >= 500:
            upsert("rounds", all_rows)
            all_rows = []
        time.sleep(0.3)  # stay under rate limit

    if all_rows:
        upsert("rounds", all_rows)


def sync_predictions(tour: str = "pga"):
    log.info(f"Syncing pre-tournament predictions ({tour})...")
    data = dg_get("preds/pre-tournament", {
        "tour":        tour,
        "odds_format": "american",
    })
    if not data:
        return
    event_id = str(data.get("event_id", "current"))
    players  = data.get("baseline", [])
    course   = {p["dg_id"]: p for p in data.get("baseline_history_fit", [])}
    rows = []
    for p in players:
        did = p.get("dg_id")
        cp  = course.get(did, {})
        rows.append({
            "dg_id":             did,
            "event_id":          event_id,
            "player_name":       p.get("player_name"),
            "tour":              tour,
            "baseline_win":      p.get("win"),
            "baseline_top5":     p.get("top_5"),
            "baseline_top10":    p.get("top_10"),
            "baseline_top20":    p.get("top_20"),
            "baseline_make_cut": p.get("make_cut"),
            "course_win":        cp.get("win"),
            "course_top5":       cp.get("top_5"),
            "course_top10":      cp.get("top_10"),
            "course_top20":      cp.get("top_20"),
            "course_make_cut":   cp.get("make_cut"),
            "updated_at":        now_utc(),
        })
    upsert("predictions", rows, "dg_id,event_id")


def sync_live_predictions(tour: str = "pga"):
    log.info(f"Syncing live predictions ({tour})...")
    data = dg_get("preds/in-play", {
        "tour":        tour,
        "dead_heat":   "false",
        "odds_format": "american",
    })
    if not data:
        return
    event_id = str(data.get("event_id", "current"))
    players  = data.get("data", [])
    rows = [
        {
            "dg_id":         p.get("dg_id"),
            "event_id":      event_id,
            "player_name":   p.get("player_name"),
            "current_pos":   p.get("current_pos"),
            "current_score": p.get("current_score"),
            "thru":          p.get("thru"),
            "win_prob":      p.get("win"),
            "top5_prob":     p.get("top_5"),
            "top10_prob":    p.get("top_10"),
            "make_cut_prob": p.get("make_cut"),
            "updated_at":    now_utc(),
        }
        for p in players
        if p.get("dg_id")
    ]
    upsert("live_predictions", rows, "dg_id,event_id")


def sync_finish_odds(tour: str = "pga"):
    """Pull DataGolf finish position odds (Win/Top5/Top10/Top20/Make Cut)
    with book odds alongside model probabilities."""
    for market in ["win", "top_5", "top_10", "top_20", "make_cut"]:
        log.info(f"Syncing finish odds ({tour} / {market})...")
        data = dg_get("betting-tools/outrights", {
            "tour":        tour,
            "market":      market,
            "odds_format": "american",
        })
        if not data:
            continue
        event_id = str(data.get("event_id", "current"))
        players  = data.get("odds", [])
        rows = []
        for p in players:
            if not isinstance(p, dict): continue
            # Find best odds (highest American = best value for bettors)
            book_cols = ["draftkings", "fanduel", "betmgm", "caesars", "bet365", "thescore", "hardrock"]
            book_odds = {k: p.get(k) for k in book_cols if p.get(k)}
            best_odds = None
            best_book = None
            if book_odds:
                # For positive odds (underdogs): highest = best value
                # For negative odds (favorites): least negative = best value
                best_book = max(book_odds, key=lambda k: book_odds[k])
                best_odds = book_odds[best_book]
            rows.append({
                "dg_id":       p.get("dg_id"),
                "event_id":    event_id,
                "market":      market,
                "player_name": p.get("player_name"),
                "tour":        tour,
                "dg_prob":     p.get("baseline_prob"),
                "dg_odds":     p.get("baseline_odds"),
                "draftkings":  p.get("draftkings"),
                "fanduel":     p.get("fanduel"),
                "betmgm":      p.get("betmgm"),
                "caesars":     p.get("caesars"),
                "bet365":      p.get("bet365"),
                "thescore":    p.get("thescore"),
                "hardrock":    p.get("hardrock"),
                "best_odds":   best_odds,
                "best_book":   best_book,
                "updated_at":  now_utc(),
            })
        upsert("finish_odds", rows)
        time.sleep(0.5)


def sync_matchup_odds(tour: str = "pga", market: str = "round_matchups"):
    log.info(f"Syncing DG matchup odds ({tour} / {market})...")
    data = dg_get("betting-tools/matchups", {
        "tour":        tour,
        "market":      market,
        "odds_format": "american",
    })
    if not data:
        return
    event_name = data.get("event_name", "")
    event_id   = str(data.get("event_id", "current"))
    round_num  = data.get("round_num", 0)
    match_list = data.get("match_list", [])

    rows = []
    for m in match_list:
        odds = m.get("odds", {})
        if not isinstance(odds, dict):
            continue
        dg_odds   = odds.get("datagolf") or {}
        if not isinstance(dg_odds, dict): dg_odds = {}
        # Collect all book lines (everything except datagolf)
        if not isinstance(odds, dict): continue
        book_lines = {k: v for k, v in odds.items() if k != "datagolf" and isinstance(v, dict)}
        # Best book odds for each side (highest American = best value)
        def best_book_odds(side):
            vals = [(book, v.get(side)) for book, v in book_lines.items() if v.get(side)]
            if not vals: return None, None
            best = max(vals, key=lambda x: float(x[1]))
            return best[0], best[1]

        p1_best_bk, p1_best = best_book_odds("p1")
        p2_best_bk, p2_best = best_book_odds("p2")

        p1_dg  = dg_odds.get("p1")
        p2_dg  = dg_odds.get("p2")

        def odds_to_prob(o):
            if not o: return None
            try:
                o = float(o)
                return round((100/(o+100)) if o > 0 else (abs(o)/(abs(o)+100)), 4)
            except: return None

        # Store all available book lines
        row = {
            "event_id":        event_id,
            "market":          market,
            "round_num":       round_num,
            "p1_dg_id":        m.get("p1_dg_id"),
            "p1_name":         m.get("p1_player_name"),
            "p2_dg_id":        m.get("p2_dg_id"),
            "p2_name":         m.get("p2_player_name"),
            "p1_dg_win_prob":  odds_to_prob(p1_dg),
            "p2_dg_win_prob":  odds_to_prob(p2_dg),
            "tie_prob":        None,
            "p1_dg_odds":      p1_dg,
            "p2_dg_odds":      p2_dg,
            # Best book line
            "p1_best_odds":    p1_best,
            "p1_best_book":    p1_best_bk,
            "p2_best_odds":    p2_best,
            "p2_best_book":    p2_best_bk,
            # Individual books
            "p1_bet365":       book_lines.get("bet365", {}).get("p1"),
            "p2_bet365":       book_lines.get("bet365", {}).get("p2"),
            "p1_betmgm":       book_lines.get("betmgm", {}).get("p1"),
            "p2_betmgm":       book_lines.get("betmgm", {}).get("p2"),
            "p1_caesars":      book_lines.get("caesars", {}).get("p1"),
            "p2_caesars":      book_lines.get("caesars", {}).get("p2"),
            "p1_draftkings":   book_lines.get("draftkings", {}).get("p1"),
            "p2_draftkings":   book_lines.get("draftkings", {}).get("p2"),
            "p1_fanduel":      book_lines.get("fanduel", {}).get("p1"),
            "p2_fanduel":      book_lines.get("fanduel", {}).get("p2"),
            "p1_thescore":     book_lines.get("thescore", {}).get("p1"),
            "p2_thescore":     book_lines.get("thescore", {}).get("p2"),
            "p1_hardrock":     book_lines.get("hardrock", {}).get("p1"),
            "p2_hardrock":     book_lines.get("hardrock", {}).get("p2"),
            "updated_at":      now_utc(),
        }
        rows.append(row)
    for row in rows:
        try:
            supabase.table("matchup_odds").upsert(
                row, on_conflict="event_id,market,round_num,p1_dg_id,p2_dg_id"
            ).execute()
        except Exception:
            pass
    log.info(f"✓  {'matchup_odds':25s} — {len(rows)} rows upserted")


def sync_historical_odds(tour: str = "pga", year: int = 2026,
                         event_id: str = "all", market: str = "round_matchups"):
    log.info(f"Syncing historical odds ({tour} {year})...")
    for book in ["draftkings", "fanduel", "betmgm", "caesars", "bet365"]:
        data = dg_get("historical-odds/matchups", {
            "tour":     tour,
            "event_id": event_id,
            "year":     year,
            "market":   market,
            "book":     book,
        })
        if not data:
            continue
        odds_list = data if isinstance(data, list) else data.get("matchups", [])
        rows = []
        for o in odds_list:
            rows.append({
                "event_id":      str(o.get("event_id", "")),
                "year":          year,
                "market":        market,
                "book":          book,
                "p1_name":       o.get("p1_player_name"),
                "p2_name":       o.get("p2_player_name"),
                "p1_open_odds":  o.get("p1_open_odds"),
                "p2_open_odds":  o.get("p2_open_odds"),
                "p1_close_odds": o.get("p1_close_odds"),
                "p2_close_odds": o.get("p2_close_odds"),
                "updated_at":    now_utc(),
            })
        if rows:
            upsert("historical_odds", rows)
        time.sleep(0.5)


def _normalize_name(name: str) -> str:
    """Convert 'First Last' to 'Last, First' to match DataGolf format."""
    if not name: return ""
    parts = name.strip().split()
    if len(parts) >= 2:
        return f"{parts[-1]}, {' '.join(parts[:-1])}"
    return name

def sync_odds_api_to_finish_odds():
    """Pull live finish position odds from The Odds API and overwrite finish_odds.
    This replaces stale DataGolf outrights data with real-time book lines."""
    log.info("Syncing finish odds from The Odds API (live lines)...")
    book_cols = ["draftkings", "fanduel", "betmgm", "caesars", "bet365", "thescore", "hardrock"]
    now = now_utc()

    for sport_key, market in GOLF_FINISH_MARKET_MAP.items():
        data = odds_get(f"sports/{sport_key}/odds", {
            "regions":    ODDS_REGION,
            "markets":    "outrights",
            "bookmakers": ",".join(ODDS_BOOKS),
            "oddsFormat": "american",
        })
        if not data:
            continue

        for event in data:
            # Build player → book odds dict
            player_odds: dict[str, dict] = {}
            for bm in event.get("bookmakers", []):
                book = bm.get("key")
                if book not in book_cols: continue
                for mkt in bm.get("markets", []):
                    for outcome in mkt.get("outcomes", []):
                        name = _normalize_name(outcome.get("name", ""))
                        price = outcome.get("price")
                        if name and price:
                            player_odds.setdefault(name, {})[book] = int(price)

            if not player_odds:
                continue

            # Build rows for finish_odds upsert
            rows = []
            for player_name, odds in player_odds.items():
                best_book = max(odds, key=lambda k: odds[k]) if odds else None
                best_odds = odds[best_book] if best_book else None
                row = {
                    "event_id":    "current",
                    "market":      market,
                    "player_name": player_name,
                    "tour":        "pga",
                    "best_odds":   best_odds,
                    "best_book":   best_book,
                    "updated_at":  now,
                }
                for col in book_cols:
                    row[col] = odds.get(col)
                rows.append(row)

            if rows:
                # Upsert by player_name + market (no dg_id available from Odds API)
                try:
                    supabase.table("finish_odds").upsert(
                        rows,
                        on_conflict="event_id,market,player_name"
                    ).execute()
                    log.info(f"  ✓ finish_odds ({market} via Odds API) — {len(rows)} rows")
                except Exception as e:
                    log.warning(f"  finish_odds upsert failed for {market}: {e}")
        time.sleep(0.3)


def sync_book_odds():
    """Pull outright winner odds for all golf majors from The Odds API.
    Round H2H matchup lines come from DataGolf (sync_matchup_odds) instead."""
    log.info("Syncing live book odds — golf outrights (The Odds API)...")
    pulled = now_utc()
    all_rows = []
    for sport in GOLF_SPORTS:
        data = odds_get(f"sports/{sport}/odds", {
            "regions":    ODDS_REGION,
            "markets":    ODDS_MARKETS,
            "bookmakers": ",".join(ODDS_BOOKS),
            "oddsFormat": "american",
        })
        if not data:
            continue
        for event in data:
            event_id = event.get("id")
            for bm in event.get("bookmakers", []):
                book = bm.get("key")
                for market in bm.get("markets", []):
                    for outcome in market.get("outcomes", []):
                        all_rows.append({
                            "event_id":    event_id,
                            "market":      f"{sport}_{market.get('key')}",
                            "bookmaker":   book,
                            "p1_name":     outcome.get("name"),
                            "p2_name":     "",
                            "p1_odds":     outcome.get("price"),
                            "p2_odds":     None,
                            "last_update": bm.get("last_update"),
                            "pulled_at":   pulled,
                        })
        time.sleep(0.5)
    upsert("book_odds", all_rows, "event_id,market,bookmaker,p1_name,p2_name")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def full_sync():
    log.info("━━━  GOLF SYNC — FULL  ━━━")
    sync_players()
    sync_schedule()
    sync_field()
    sync_skill_ratings()
    sync_rounds(year=2026)
    sync_rounds(year=2025)
    sync_predictions()
    sync_finish_odds()
    sync_odds_api_to_finish_odds()
    sync_matchup_odds()
    sync_historical_odds(year=2026)
    sync_historical_odds(year=2025)
    sync_book_odds()
    log.info("━━━  FULL SYNC COMPLETE  ━━━")


def live_sync():
    log.info("━━━  GOLF SYNC — LIVE  ━━━")
    sync_field()
    sync_skill_ratings()
    sync_live_predictions()
    sync_finish_odds()
    sync_odds_api_to_finish_odds()
    sync_matchup_odds()
    sync_book_odds()
    log.info("━━━  LIVE SYNC COMPLETE  ━━━")


def pre_sync():
    log.info("━━━  GOLF SYNC — PRE-TOURNAMENT  ━━━")
    sync_players()
    sync_schedule()
    sync_field()
    sync_skill_ratings()
    sync_predictions()
    sync_finish_odds()
    sync_matchup_odds(market="tournament_matchups")
    sync_matchup_odds(market="round_matchups")
    sync_book_odds()
    log.info("━━━  PRE-TOURNAMENT SYNC COMPLETE  ━━━")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Golf Model Sync Script")
    parser.add_argument(
        "--mode",
        choices=["full", "live", "pre", "setup"],
        default="full",
        help="Sync mode: full | live | pre | setup"
    )
    args = parser.parse_args()

    if args.mode == "setup":
        setup_tables()
    elif args.mode == "live":
        live_sync()
    elif args.mode == "pre":
        pre_sync()
    else:
        full_sync()
