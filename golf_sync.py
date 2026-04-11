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
    "thescore", "betmgm", "caesars"
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


def american_to_prob(val) -> float | None:
    """Convert American odds (int, float, or string) to implied probability (0–1 decimal).
    Returns None for 'n/a', None, empty, or invalid values."""
    if val is None:
        return None
    try:
        o = float(val)
    except (TypeError, ValueError):
        return None  # handles 'n/a', '', etc.
    if o == 0:
        return None
    if o > 0:
        return round(100 / (o + 100), 6)
    else:
        return round(abs(o) / (abs(o) + 100), 6)


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
-- Probs stored as decimal (0–1). App multiplies by 100 for display.
-- Raw American odds stored in *_odds columns for reference.
create table if not exists live_predictions (
    dg_id           integer,
    event_id        text,
    player_name     text,
    current_pos     integer,
    current_score   integer,
    thru            integer,
    today           integer,
    round_num       integer,
    win_prob        numeric,   -- decimal 0–1, e.g. 0.185
    top5_prob       numeric,
    top10_prob      numeric,
    top20_prob      numeric,
    make_cut_prob   numeric,
    win_odds        text,      -- raw American e.g. "-225"
    top5_odds       text,
    top10_odds      text,
    top20_odds      text,
    make_cut_odds   text,
    updated_at      timestamptz default now(),
    primary key (dg_id, event_id)
);

-- If upgrading existing live_predictions table, run these ALTERs:
-- alter table live_predictions add column if not exists top20_prob numeric;
-- alter table live_predictions add column if not exists today integer;
-- alter table live_predictions add column if not exists round_num integer;
-- alter table live_predictions add column if not exists win_odds text;
-- alter table live_predictions add column if not exists top5_odds text;
-- alter table live_predictions add column if not exists top10_odds text;
-- alter table live_predictions add column if not exists top20_odds text;
-- alter table live_predictions add column if not exists make_cut_odds text;

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

-- Tournament results — final finish positions for each event
-- Used to auto-settle bets and calibrate model accuracy
create table if not exists tournament_results (
    id              bigserial primary key,
    dg_id           integer,
    event_id        text,
    event_name      text,
    season          integer,
    player_name     text,
    finish_pos      integer,       -- official finish position (1 = win)
    finish_pos_text text,          -- e.g. "T4", "MC", "WD"
    made_cut        boolean,
    top_5           boolean,
    top_10          boolean,
    top_20          boolean,
    score_r1        integer,
    score_r2        integer,
    score_r3        integer,
    score_r4        integer,
    total_score     integer,
    pre_win_prob    numeric,       -- model probability at time of prediction
    pre_top10_prob  numeric,
    updated_at      timestamptz default now(),
    unique (dg_id, event_id)
);

-- Edge accuracy log — track every sharp play the model surfaces
-- One row per player+market+sync that clears the sharp threshold
-- Allows end-of-season calibration: did 5% edge plays actually hit at +EV?
create table if not exists edge_log (
    id              bigserial primary key,
    event_id        text,
    event_name      text,
    dg_id           integer,
    player_name     text,
    market          text,         -- win, top_5, top_10, top_20, make_cut, h2h
    model_prob      numeric,      -- decimal 0–1
    book_implied    numeric,      -- decimal 0–1
    edge_pct        numeric,      -- model_prob - book_implied, in %
    edge_tier       text,         -- STRONG / SHARP / VALUE
    best_odds       integer,
    best_book       text,
    is_live         boolean,      -- true if model_prob from live_predictions
    outcome         text,         -- WIN / LOSS / PUSH / NULL (pending)
    logged_at       timestamptz default now()
);

-- Prediction snapshots — archive model state at start of each tournament
-- Lets you compare pre-tourney model probs to final results over time
create table if not exists prediction_snapshots (
    id              bigserial primary key,
    event_id        text,
    event_name      text,
    season          integer,
    snapshot_type   text,         -- 'pre' or 'live_r1', 'live_r2', etc.
    dg_id           integer,
    player_name     text,
    win_prob        numeric,
    top5_prob       numeric,
    top10_prob      numeric,
    top20_prob      numeric,
    make_cut_prob   numeric,
    current_pos     integer,      -- null for pre-tourney
    current_score   integer,      -- null for pre-tourney
    snapped_at      timestamptz default now(),
    unique (event_id, snapshot_type, dg_id)
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
    """Pull DataGolf in-play predictions and store as decimal probabilities (0–1).
    
    DataGolf returns American odds strings e.g. "-225", "+450", or "n/a".
    We convert all to decimal probability before storing so the app can do
    simple arithmetic (prob * 100 = percent) without re-converting.
    
    Supabase live_predictions schema expects:
      win_prob, top5_prob, top10_prob, top20_prob, make_cut_prob  → float/numeric (0–1)
      win_odds, top5_odds, top10_odds, top20_odds, make_cut_odds  → text (raw American)
      current_pos, current_score, thru, today, round_num          → integer/text
    """
    log.info(f"Syncing live predictions ({tour})...")
    data = dg_get("preds/in-play", {
        "tour":        tour,
        "dead_heat":   "false",
        "odds_format": "american",
    })
    if not data:
        log.error("sync_live_predictions: dg_get returned None")
        return

    event_id = str(data.get("event_id", "current"))
    players  = data.get("data", [])

    if not players:
        log.warning(f"sync_live_predictions: empty data — response keys: {list(data.keys())}")
        return

    rows = []
    for p in players:
        if not p.get("dg_id"):
            continue

        # Raw American odds (stored as text, 'n/a' preserved)
        raw_win  = p.get("win")
        raw_t5   = p.get("top_5")
        raw_t10  = p.get("top_10")
        raw_t20  = p.get("top_20")
        raw_cut  = p.get("make_cut")

        # Numeric score fields — DataGolf sometimes returns these as strings too
        def safe_int(v):
            try: return int(v)
            except (TypeError, ValueError): return None

        rows.append({
            "dg_id":          p.get("dg_id"),
            "event_id":       event_id,
            "player_name":    p.get("player_name"),
            "current_pos":    safe_int(p.get("current_pos")),
            "current_score":  safe_int(p.get("current_score")),
            "thru":           safe_int(p.get("thru")),
            "today":          safe_int(p.get("today")),
            "round_num":      safe_int(p.get("round")),
            # Decimal probabilities (0–1) for arithmetic in the app
            "win_prob":       american_to_prob(raw_win),
            "top5_prob":      american_to_prob(raw_t5),
            "top10_prob":     american_to_prob(raw_t10),
            "top20_prob":     american_to_prob(raw_t20),
            "make_cut_prob":  american_to_prob(raw_cut),
            # Raw American odds stored as text — useful for display
            "win_odds":       str(raw_win)  if raw_win  is not None else None,
            "top5_odds":      str(raw_t5)   if raw_t5   is not None else None,
            "top10_odds":     str(raw_t10)  if raw_t10  is not None else None,
            "top20_odds":     str(raw_t20)  if raw_t20  is not None else None,
            "make_cut_odds":  str(raw_cut)  if raw_cut  is not None else None,
            "updated_at":     now_utc(),
        })

    log.info(f"sync_live_predictions: {len(rows)} players — sample: {rows[0] if rows else 'none'}")
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
            book_cols = ["draftkings", "fanduel", "betmgm", "caesars", "thescore", "hardrock"]
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
        if not isinstance(m, dict):
            continue
        odds = m.get("odds", {})
        if not isinstance(odds, dict):
            continue
        dg_odds   = odds.get("datagolf", {})
        # Collect all book lines (everything except datagolf)
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
    for book in ["draftkings", "fanduel", "betmgm", "caesars"]:
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
    """Pull live finish position odds from The Odds API and upsert into finish_odds.
    
    Covers all 4 majors across all 5 markets (win/top5/top10/top20/make_cut).
    Uses player_name as the match key since Odds API has no dg_id.
    
    IMPORTANT: finish_odds table needs a unique constraint on (event_id, market, player_name)
    in addition to (or instead of) the dg_id-based primary key.
    Run this in Supabase SQL editor once:
      alter table finish_odds add column if not exists dg_id integer default null;
      create unique index if not exists finish_odds_name_idx
        on finish_odds (event_id, market, player_name);
    """
    log.info("Syncing finish odds from The Odds API (live lines)...")
    book_cols = ["draftkings", "fanduel", "betmgm", "caesars", "thescore", "hardrock"]
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
            player_odds: dict[str, dict] = {}
            for bm in event.get("bookmakers", []):
                book = bm.get("key")
                if book not in book_cols:
                    continue
                for mkt in bm.get("markets", []):
                    for outcome in mkt.get("outcomes", []):
                        name  = _normalize_name(outcome.get("name", ""))
                        price = outcome.get("price")
                        if name and price:
                            player_odds.setdefault(name, {})[book] = int(price)

            if not player_odds:
                continue

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
                try:
                    # Upsert by (event_id, market, player_name) — dg_id may be null
                    supabase.table("finish_odds").upsert(
                        rows,
                        on_conflict="event_id,market,player_name"
                    ).execute()
                    log.info(f"  ✓ finish_odds ({sport_key} → {market}) — {len(rows)} rows")
                except Exception as e:
                    log.warning(f"  finish_odds upsert failed for {sport_key}/{market}: {e}")
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


def snapshot_predictions(snapshot_type: str = "pre", tour: str = "pga"):
    """Archive current predictions into prediction_snapshots for season-long calibration.
    
    Call with snapshot_type='pre' before tournament starts.
    Call with snapshot_type='live_r1', 'live_r2' etc. during rounds.
    """
    log.info(f"Snapshotting predictions ({snapshot_type})...")
    if snapshot_type == "pre":
        data = dg_get("preds/pre-tournament", {"tour": tour, "odds_format": "american"})
        if not data:
            return
        event_id   = str(data.get("event_id", "current"))
        event_name = data.get("event_name", "")
        players    = data.get("baseline", [])
        course     = {p["dg_id"]: p for p in data.get("baseline_history_fit", [])}

        # Grab schedule for event_name if missing
        rows = []
        for p in players:
            did = p.get("dg_id")
            cp  = course.get(did, {})
            rows.append({
                "event_id":       event_id,
                "event_name":     event_name,
                "season":         2026,
                "snapshot_type":  snapshot_type,
                "dg_id":          did,
                "player_name":    p.get("player_name"),
                "win_prob":       american_to_prob(p.get("win")),
                "top5_prob":      american_to_prob(p.get("top_5")),
                "top10_prob":     american_to_prob(p.get("top_10")),
                "top20_prob":     american_to_prob(p.get("top_20")),
                "make_cut_prob":  american_to_prob(p.get("make_cut")),
                "current_pos":    None,
                "current_score":  None,
            })
    else:
        # Live snapshot from in-play data
        data = dg_get("preds/in-play", {"tour": tour, "dead_heat": "false", "odds_format": "american"})
        if not data:
            return
        event_id   = str(data.get("event_id", "current"))
        event_name = data.get("event_name", "")
        players    = data.get("data", [])
        rows = []
        for p in players:
            def safe_int(v):
                try: return int(v)
                except: return None
            rows.append({
                "event_id":       event_id,
                "event_name":     event_name,
                "season":         2026,
                "snapshot_type":  snapshot_type,
                "dg_id":          p.get("dg_id"),
                "player_name":    p.get("player_name"),
                "win_prob":       american_to_prob(p.get("win")),
                "top5_prob":      american_to_prob(p.get("top_5")),
                "top10_prob":     american_to_prob(p.get("top_10")),
                "top20_prob":     american_to_prob(p.get("top_20")),
                "make_cut_prob":  american_to_prob(p.get("make_cut")),
                "current_pos":    safe_int(p.get("current_pos")),
                "current_score":  safe_int(p.get("current_score")),
            })

    upsert("prediction_snapshots", rows, "event_id,snapshot_type,dg_id")


def sync_tournament_results(tour: str = "pga", event_id: str = "current"):
    """Pull final leaderboard results after tournament ends and store in tournament_results.
    Also auto-settles any matching Pending bets.
    
    Call this after Sunday's final round completes.
    """
    log.info(f"Syncing tournament results ({tour} event={event_id})...")
    data = dg_get("preds/in-play", {"tour": tour, "dead_heat": "false", "odds_format": "american"})
    if not data:
        return

    ev_id      = str(data.get("event_id", event_id))
    event_name = data.get("event_name", "")
    players    = data.get("data", [])

    # Grab pre-tourney probs for comparison
    pre_snap = supabase.table("prediction_snapshots") \
        .select("dg_id, win_prob, top10_prob") \
        .eq("event_id", ev_id) \
        .eq("snapshot_type", "pre") \
        .execute().data
    pre_by_id = {r["dg_id"]: r for r in pre_snap}

    rows = []
    for p in players:
        did   = p.get("dg_id")
        pos   = p.get("current_pos")
        score = p.get("current_score")
        try: pos_int = int(pos) if pos not in (None, "n/a", "") else None
        except: pos_int = None

        pre = pre_by_id.get(did, {})
        rows.append({
            "dg_id":          did,
            "event_id":       ev_id,
            "event_name":     event_name,
            "season":         2026,
            "player_name":    p.get("player_name"),
            "finish_pos":     pos_int,
            "finish_pos_text":str(pos) if pos else None,
            "made_cut":       p.get("make_cut") != "n/a",
            "top_5":          pos_int is not None and pos_int <= 5,
            "top_10":         pos_int is not None and pos_int <= 10,
            "top_20":         pos_int is not None and pos_int <= 20,
            "score_r1":       p.get("R1"),
            "score_r2":       p.get("R2"),
            "score_r3":       p.get("R3"),
            "score_r4":       p.get("R4"),
            "total_score":    score,
            "pre_win_prob":   pre.get("win_prob"),
            "pre_top10_prob": pre.get("top10_prob"),
            "updated_at":     now_utc(),
        })

    if rows:
        upsert("tournament_results", rows, "dg_id,event_id")
        log.info(f"tournament_results — {len(rows)} rows")
        # Auto-settle pending bets for this event
        _auto_settle_bets(ev_id, rows)


def _auto_settle_bets(event_id: str, results: list[dict]):
    """Auto-settle Pending bets by matching player_name + market against tournament_results."""
    try:
        pending = supabase.table("bets") \
            .select("*") \
            .eq("result", "Pending") \
            .execute().data
    except Exception as e:
        log.error(f"auto_settle: could not fetch pending bets: {e}")
        return

    results_by_name = {r["player_name"]: r for r in results}
    settled_count = 0

    for bet in pending:
        player = bet.get("player_name", "")
        market = (bet.get("market") or "").lower()
        result_row = results_by_name.get(player)
        if not result_row:
            continue  # player name mismatch — skip, settle manually

        outcome = None
        if market in ("win", "tournament winner"):
            outcome = "Win" if result_row.get("finish_pos") == 1 else "Loss"
        elif market in ("top 5", "top5"):
            outcome = "Win" if result_row.get("top_5") else "Loss"
        elif market in ("top 10", "top10"):
            outcome = "Win" if result_row.get("top_10") else "Loss"
        elif market in ("top 20", "top20"):
            outcome = "Win" if result_row.get("top_20") else "Loss"
        elif market in ("make cut", "make_cut"):
            outcome = "Win" if result_row.get("made_cut") else "Loss"

        if not outcome:
            continue  # H2H or unknown market — skip

        odds    = float(bet.get("odds", 0))
        stake   = float(bet.get("stake", 0))
        pl = round((stake * odds / 100) if (outcome == "Win" and odds > 0)
                   else (stake * 100 / abs(odds)) if (outcome == "Win" and odds < 0)
                   else -stake, 2)

        try:
            supabase.table("bets").update({
                "result":       outcome,
                "profit_loss":  pl,
            }).eq("id", bet["id"]).execute()
            settled_count += 1
        except Exception as e:
            log.error(f"auto_settle: failed to update bet #{bet['id']}: {e}")

    log.info(f"auto_settle: settled {settled_count}/{len(pending)} pending bets")


def log_sharp_plays():
    """Snapshot all current sharp plays into edge_log for season-long calibration.
    Call once per sync during rounds. Outcome will be filled by _auto_settle_bets.
    """
    log.info("Logging sharp plays to edge_log...")
    try:
        fin_odds = supabase.table("finish_odds").select("*").execute().data
        live_preds = supabase.table("live_predictions").select("*").execute().data
        pre_preds  = supabase.table("predictions").select("*").execute().data
    except Exception as e:
        log.error(f"log_sharp_plays: fetch failed: {e}")
        return

    live_by_id = {r["dg_id"]: r for r in live_preds}
    pre_by_id  = {(r["dg_id"], r.get("event_id")): r for r in pre_preds}

    THRESHOLDS = {"win": 2.0, "top_5": 3.0, "top_10": 3.5, "top_20": 4.0, "make_cut": 5.0}
    BOOK_COLS  = ["draftkings", "fanduel", "betmgm", "caesars", "thescore", "hardrock"]

    rows = []
    seen = set()
    for fo in fin_odds:
        dg_id   = fo.get("dg_id")
        market  = fo.get("market")
        event_id = fo.get("event_id")
        if not dg_id or not market:
            continue

        # Get model prob — live if available, else pre-tourney
        lp  = live_by_id.get(dg_id, {})
        live_prob_map = {"win": lp.get("win_prob"), "top_5": lp.get("top5_prob"),
                         "top_10": lp.get("top10_prob"), "top_20": lp.get("top20_prob"),
                         "make_cut": lp.get("make_cut_prob")}
        model_prob = live_prob_map.get(market)
        is_live = model_prob is not None

        if not model_prob:
            # Fall back to pre-tourney
            pp = pre_by_id.get((dg_id, event_id), {})
            pre_map = {"win": pp.get("baseline_win"), "top_5": pp.get("baseline_top5"),
                       "top_10": pp.get("baseline_top10"), "top_20": pp.get("baseline_top20"),
                       "make_cut": pp.get("baseline_make_cut")}
            raw = pre_map.get(market)
            model_prob = american_to_prob(raw) if raw else None

        if not model_prob:
            continue

        threshold = THRESHOLDS.get(market, 2.0)

        # Check each book
        for col in BOOK_COLS:
            book_odds = fo.get(col)
            if not book_odds:
                continue
            book_impl = american_to_prob(book_odds)
            if not book_impl:
                continue
            edge = round((model_prob - book_impl) * 100, 3)
            if edge < threshold:
                continue

            tier = ("STRONG" if edge >= threshold + 3 else
                    "SHARP"  if edge >= threshold + 1 else "VALUE")

            key = (dg_id, market, col, event_id)
            if key in seen:
                continue
            seen.add(key)

            rows.append({
                "event_id":    event_id,
                "dg_id":       dg_id,
                "player_name": fo.get("player_name"),
                "market":      market,
                "model_prob":  round(model_prob, 6),
                "book_implied":round(book_impl, 6),
                "edge_pct":    edge,
                "edge_tier":   tier,
                "best_odds":   int(book_odds),
                "best_book":   col,
                "is_live":     is_live,
                "outcome":     None,
                "logged_at":   now_utc(),
            })

    if rows:
        try:
            supabase.table("edge_log").insert(rows).execute()
            log.info(f"edge_log — {len(rows)} sharp plays logged")
        except Exception as e:
            log.error(f"edge_log insert failed: {e}")


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
    log_sharp_plays()
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
    log_sharp_plays()
    log.info("━━━  LIVE SYNC COMPLETE  ━━━")


def pre_sync():
    log.info("━━━  GOLF SYNC — PRE-TOURNAMENT  ━━━")
    sync_players()
    sync_schedule()
    sync_field()
    sync_skill_ratings()
    sync_predictions()
    snapshot_predictions(snapshot_type="pre")
    sync_finish_odds()
    sync_matchup_odds(market="tournament_matchups")
    sync_matchup_odds(market="round_matchups")
    sync_book_odds()
    log.info("━━━  PRE-TOURNAMENT SYNC COMPLETE  ━━━")


def results_sync():
    """Run after Sunday's final round to settle bets and archive results."""
    log.info("━━━  GOLF SYNC — RESULTS  ━━━")
    sync_tournament_results()
    log.info("━━━  RESULTS SYNC COMPLETE  ━━━")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Golf Model Sync Script")
    parser.add_argument(
        "--mode",
        choices=["full", "live", "pre", "results", "setup"],
        default="full",
        help="Sync mode: full | live | pre | results | setup"
    )
    args = parser.parse_args()

    if args.mode == "setup":
        setup_tables()
    elif args.mode == "live":
        live_sync()
    elif args.mode == "pre":
        pre_sync()
    elif args.mode == "results":
        results_sync()
    else:
        full_sync()
