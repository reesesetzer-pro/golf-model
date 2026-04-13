"""
golf_app.py — Golf H2H Betting Model Dashboard
Deploy to Streamlit Cloud via GitHub (same repo as MLB F5 model)
Run locally: streamlit run golf_app.py
"""

import re
import streamlit as st
import pandas as pd
from supabase import create_client
from datetime import datetime, timezone, timedelta

def now_et():
    utc_now = datetime.now(timezone.utc)
    # EDT = UTC-4, EST = UTC-5. April = EDT
    et_now = utc_now - timedelta(hours=4)
    return et_now.strftime("%I:%M %p ET")

def quick_log_bet(player, market, book, odds, edge, stake=10.0, notes="", event=""):
    """One-click bet logging from any sharp play list."""
    try:
        sb = get_supabase()
        o   = float(odds)
        imp = round((100/(o+100)*100) if o > 0 else (abs(o)/(abs(o)+100)*100), 2)
        win = round((stake * o / 100) if o > 0 else (stake * 100 / abs(o)), 2)
        tier = "🔥🔥 STRONG (5%+)" if edge >= 5 else ("🔥 SHARP (3-5%)" if edge >= 3 else "✅ VALUE (2-3%)")
        sb.table("bets").insert({
            "player_name":  player,
            "market":       market,
            "side":         f"{player} {market}",
            "book":         book,
            "odds":         int(o),
            "stake":        stake,
            "to_win":       win,
            "implied_prob": imp,
            "edge_at_bet":  round(edge, 2),
            "round":        "Live",
            "notes":        f"[{tier}] [{event}] {notes}",
            "result":       "Pending",
            "profit_loss":  0.0,
            "logged_at":    (datetime.now(timezone.utc) - timedelta(hours=4)).isoformat(),
        }).execute()
        return True
    except Exception as e:
        return False


st.set_page_config(
    page_title="Golf Betting Model",
    page_icon="⛳",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Auto-refresh ────────────────────────────────────────────────────────────────
from streamlit_autorefresh import st_autorefresh
# Refresh options in sidebar
with st.sidebar:
    st.markdown("### ⚙️ Settings")
    refresh_mins = st.selectbox(
        "Auto-refresh interval",
        [5, 10, 15, 30, 60, 0],
        index=2,
        format_func=lambda x: f"Every {x} min" if x > 0 else "Off"
    )
    if refresh_mins > 0:
        st_autorefresh(interval=refresh_mins * 60 * 1000, key="golf_refresh")
        st.caption(f"🔄 Refreshing every {refresh_mins} min")

    st.markdown("---")
    st.markdown("### 💰 Quick Bet Settings")
    default_stake = st.number_input("Default Stake ($)", value=10.0, step=5.0, key="default_stake")
    st.caption("Used when clicking 🎯 Take It on any play")
    bankroll_setting = st.number_input("Bankroll ($)", value=500.0, step=100.0, key="bankroll")
    st.caption("Used for Kelly stake sizing in the Alerts tab")

# ── Theme / CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
    background-color: #0f1a0f;
    color: #e8f5e9;
}

/* Header */
.golf-header {
    background: linear-gradient(135deg, #1b5e20 0%, #2e7d32 50%, #1b5e20 100%);
    border-bottom: 2px solid #4caf50;
    padding: 1.5rem 2rem;
    margin: -1rem -1rem 1.5rem -1rem;
    display: flex;
    align-items: center;
    justify-content: space-between;
}
.golf-header h1 {
    font-family: 'DM Serif Display', serif;
    font-size: 2rem;
    color: #fff;
    margin: 0;
    letter-spacing: 0.02em;
}
.golf-header .subtitle {
    font-size: 0.8rem;
    color: #a5d6a7;
    margin-top: 0.2rem;
}
.badge {
    background: #4caf50;
    color: #fff;
    font-size: 0.72rem;
    font-weight: 600;
    padding: 0.25rem 0.7rem;
    border-radius: 20px;
    letter-spacing: 0.05em;
}

/* Metric cards */
.metric-row {
    display: flex;
    gap: 1rem;
    margin-bottom: 1.5rem;
}
.metric-card {
    background: #1a2e1a;
    border: 1px solid #2e7d32;
    border-radius: 10px;
    padding: 1rem 1.4rem;
    flex: 1;
    min-width: 0;
}
.metric-card .label {
    font-size: 0.7rem;
    color: #81c784;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 0.3rem;
}
.metric-card .value {
    font-family: 'DM Serif Display', serif;
    font-size: 1.6rem;
    color: #fff;
    line-height: 1;
}
.metric-card .sub {
    font-size: 0.72rem;
    color: #66bb6a;
    margin-top: 0.2rem;
}

/* Tabs */
.stTabs [data-baseweb="tab-list"] {
    background: #1a2e1a;
    border-radius: 10px;
    padding: 0.3rem;
    gap: 0.2rem;
    border: 1px solid #2e7d32;
}
.stTabs [data-baseweb="tab"] {
    background: transparent;
    color: #81c784;
    border-radius: 8px;
    padding: 0.5rem 1.2rem;
    font-size: 0.82rem;
    font-weight: 500;
    border: none;
}
.stTabs [aria-selected="true"] {
    background: #2e7d32 !important;
    color: #fff !important;
}

/* Dataframe */
.stDataFrame { border-radius: 10px; overflow: hidden; }
[data-testid="stDataFrameResizable"] {
    border: 1px solid #2e7d32;
    border-radius: 10px;
}

/* Positive / negative edge pills */
.edge-pos { color: #69f0ae; font-weight: 600; }
.edge-neg { color: #ef9a9a; font-weight: 600; }
.edge-neu { color: #90a4ae; }

/* Section headers */
.section-header {
    font-family: 'DM Serif Display', serif;
    font-size: 1.1rem;
    color: #a5d6a7;
    border-bottom: 1px solid #2e7d32;
    padding-bottom: 0.4rem;
    margin: 1.2rem 0 0.8rem 0;
}

/* Player comparison card */
.compare-card {
    background: #1a2e1a;
    border: 1px solid #2e7d32;
    border-radius: 12px;
    padding: 1.2rem;
}
.compare-card .player-name {
    font-family: 'DM Serif Display', serif;
    font-size: 1.3rem;
    color: #fff;
}
.compare-card .player-rank {
    font-size: 0.78rem;
    color: #81c784;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background: #0f1a0f;
    border-right: 1px solid #2e7d32;
}

/* Buttons */
.stButton button {
    background: #2e7d32;
    color: #fff;
    border: none;
    border-radius: 8px;
    font-weight: 500;
}
.stButton button:hover {
    background: #388e3c;
    border: none;
}

/* Selectbox */
.stSelectbox > div > div {
    background: #1a2e1a;
    border: 1px solid #2e7d32;
    border-radius: 8px;
    color: #e8f5e9;
}

/* Info box */
.info-box {
    background: #1a2e1a;
    border-left: 3px solid #4caf50;
    border-radius: 0 8px 8px 0;
    padding: 0.8rem 1rem;
    font-size: 0.82rem;
    color: #a5d6a7;
    margin: 0.8rem 0;
}

/* Table header override */
thead tr th {
    background: #2e7d32 !important;
    color: #fff !important;
    font-weight: 600 !important;
}
</style>
""", unsafe_allow_html=True)

# ── Credentials ─────────────────────────────────────────────────────────────────
@st.cache_resource
def get_supabase():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

# ── Data loading ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)  # cache 5 min
def load_data():
    sb = get_supabase()
    skill      = sb.table("skill_ratings").select("*").order("dg_rank").execute().data
    field      = sb.table("field").select("*").execute().data
    preds      = sb.table("predictions").select("*").execute().data
    live_preds = sb.table("live_predictions").select("*").execute().data
    fin_odds_raw = sb.table("finish_odds").select("*").order("updated_at", desc=True).execute().data
    # Deduplicate by (dg_id, market) — keep most recently updated row per player+market
    _seen = set()
    fin_odds = []
    for r in fin_odds_raw:
        key = (r.get("dg_id"), r.get("market"))
        if key not in _seen:
            _seen.add(key)
            fin_odds.append(r)
    matchups   = sb.table("matchup_odds").select("*").order("p1_dg_win_prob", desc=True).execute().data
    rounds     = sb.table("rounds").select("*").order("year", desc=True).limit(25000).execute().data
    schedule   = sb.table("schedule").select("*").order("start_date", desc=True).execute().data
    return skill, field, preds, live_preds, fin_odds, matchups, rounds, schedule

def edge_tier(e):
    e = float(e or 0)
    if e >= 5:   return "🔥🔥 STRONG (5%+)"
    if e >= 3:   return "🔥 SHARP (3-5%)"
    if e >= 2:   return "✅ VALUE (2-3%)"
    if e >= 0:   return "Below Threshold"
    return "Manual / No Edge"

def american_to_implied(odds):
    if not odds: return None
    try:
        o = float(odds)
        return round((100/(o+100)*100) if o > 0 else (abs(o)/(abs(o)+100)*100), 2)
    except: return None

def edge_pct(dg_prob_pct, book_odds):
    """dg_prob_pct is in % form (e.g. 18.5), book_odds is American.
    Edge = DG model prob% - book implied prob%"""
    if not dg_prob_pct or not book_odds: return None
    imp = american_to_implied(book_odds)
    return round(dg_prob_pct - imp, 2) if imp else None

def color_sharp(val):
    if isinstance(val, str):
        if "STRONG" in val: return "background-color:#1a3a1a; color:#69f0ae; font-weight:700"
        if "SHARP"  in val: return "background-color:#1e3320; color:#a5d6a7; font-weight:600"
        if "VALUE"  in val: return "background-color:#1b2e1b; color:#81c784"
    return ""

SHARP_THRESHOLDS = {
    "win":      2.0,
    "top_5":    3.0,
    "top_10":   3.5,
    "top_20":   4.0,
    "make_cut": 5.0,
    "matchup":  2.0,
}

PRED_LABELS = {"🔥🔥 STRONG": "📈 HIGH", "🔥 SHARP": "📊 MED", "✅ VALUE": "📉 LOW"}

def sharp_value(dg_prob_pct, book_odds, market="win"):
    """Returns (edge%, label) using market-specific thresholds."""
    e = edge_pct(dg_prob_pct, book_odds)
    if e is None: return None, None
    threshold = SHARP_THRESHOLDS.get(market, 2.0)
    if e >= threshold + 3.0: return e, "🔥🔥 STRONG"
    if e >= threshold + 1.0: return e, "🔥 SHARP"
    if e >= threshold:       return e, "✅ VALUE"
    return e, None

def fmt_odds(val):
    if val is None: return "—"
    return f"+{int(val)}" if val > 0 else str(int(val))

def fmt_pct(val):
    if val is None: return "—"
    return f"{val*100:.2f}%"

def fmt_prob(val):
    if val is None: return "—"
    return f"{val:.2f}%"

# ── Header ───────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="golf-header">
  <div>
    <h1>⛳ Golf Betting Model</h1>
    <div class="subtitle">DataGolf + The Odds API · Live Tournament Intelligence</div>
  </div>
  <div class="badge">LIVE DATA</div>
</div>
""", unsafe_allow_html=True)

# ── Load data ────────────────────────────────────────────────────────────────────
with st.spinner("Loading live data..."):
    try:
        skill, field, preds, live_preds, fin_odds, matchups, rounds, schedule = load_data()
        live_pred_by_id = {int(p["dg_id"]): p for p in live_preds if p.get("dg_id")}
    except Exception as e:
        st.error(f"Could not connect to database: {e}")
        st.stop()

# Index data
skill_by_id  = {int(p["dg_id"]): p for p in skill if p.get("dg_id")}
field_ids    = {int(p["dg_id"]) for p in field if p.get("dg_id")}
pred_by_id   = {int(p["dg_id"]): p for p in preds if p.get("dg_id")}
fo_index     = {(int(fo["dg_id"]), fo["market"]): fo for fo in fin_odds if fo.get("dg_id")}

# Detect round / tournament state
_lp_active      = [p for p in live_preds if p.get("dg_id")]
_n_finished     = sum(1 for p in _lp_active if (p.get("thru") or 0) >= 18)
tournament_complete = bool(_lp_active) and (_n_finished >= len(_lp_active) * 0.85)
round_in_progress   = (
    not tournament_complete
    and any((p.get("thru") or 0) > 0 for p in _lp_active)
)

# Current event
current_event = "Current Event"
current_event_id = None
if field:
    current_event_id = field[0].get("event_id")
    for e in schedule:
        if str(e.get("event_id")) == str(current_event_id):
            current_event = e.get("event_name", current_event)
            break

# Course rounds
course_rounds = {}
for r in rounds:
    if str(r.get("event_id")) == str(current_event_id):
        did = r.get("dg_id")
        course_rounds.setdefault(did, []).append(r)

# Build field player list
def implied_from_best(fo):
    """Return best available American odds from any book column."""
    for key in ["draftkings", "fanduel", "betmgm", "caesars", "bet365", "thescore", "hardrock", "best_odds"]:
        val = fo.get(key)
        if val is not None and val != 0:
            try:
                if float(val) != 0:
                    return val
            except:
                pass
    return None

# Build field player list
field_players = []
for p in [x for x in field if not x.get("withdrawn")]:
    did  = int(p["dg_id"]) if p.get("dg_id") else None
    if not did: continue
    sk   = skill_by_id.get(did, {})
    pr   = pred_by_id.get(did, {})
    fo_w = fo_index.get((did, "win"),      {})
    fo_5 = fo_index.get((did, "top_5"),    {})
    fo_10= fo_index.get((did, "top_10"),   {})
    fo_20= fo_index.get((did, "top_20"),   {})
    fo_c = fo_index.get((did, "make_cut"), {})

    # Predictions stored as American odds (odds_format=american in sync)
    # Use american_to_implied to convert to probability %
    def pred_pct(val):
        if val is None: return None
        return american_to_implied(val)  # returns % e.g. 18.5 for +450 odds

    w_prob   = pred_pct(pr.get("baseline_win"))
    t5_prob  = pred_pct(pr.get("baseline_top5"))
    t10_prob = pred_pct(pr.get("baseline_top10"))
    t20_prob = pred_pct(pr.get("baseline_top20"))
    c_prob   = pred_pct(pr.get("baseline_make_cut"))
    cw_prob  = pred_pct(pr.get("course_win"))

    # Override probs with live predictions once a player has teed off
    lp = live_pred_by_id.get(did, {})
    live_thru = lp.get("thru") or 0
    has_started = live_thru > 0
    current_pos = lp.get("current_pos") or 999

    live_win  = american_to_implied(lp.get("win_prob"))  if (lp.get("win_prob")  and has_started) else None
    live_t5   = american_to_implied(lp.get("top5_prob")) if (lp.get("top5_prob") and has_started) else None
    live_t10  = american_to_implied(lp.get("top10_prob"))if (lp.get("top10_prob")and has_started) else None

    # Once a player has teed off, never fall back to pre-tournament baseline
    w_prob_display  = live_win  if has_started else w_prob
    t5_prob_display = live_t5   if has_started else t5_prob
    t10_prob_display= live_t10  if has_started else t10_prob
    # Top 20: no live prob from DG — suppress if player is clearly out of contention
    t20_prob_display = None if (has_started and current_pos > 30) else t20_prob
    w_prob_is_live = has_started

    # Best book odds
    w_best_odds  = implied_from_best(fo_w)
    t5_best_odds = implied_from_best(fo_5)
    t10_best_odds= implied_from_best(fo_10)

    dg_rank = sk.get("dg_rank") or sk.get("rank")

    field_players.append({
        "dg_id": did, "name": p.get("player_name",""),
        "dg_rank": dg_rank,
        "sg_total": sk.get("sg_total"), "sg_ott": sk.get("sg_ott"),
        "sg_app":   sk.get("sg_app"),   "sg_atg": sk.get("sg_atg"),
        "sg_putt":  sk.get("sg_putt"),
        "bl_win":   w_prob_display,   "bl_top5":  t5_prob_display,
        "bl_top10": t10_prob_display, "bl_cut":   c_prob,
        "co_win":   cw_prob,
        # _p keys used by finish odds tab (prob_key = f"{mk}_p")
        "w_p":   w_prob_display,
        "t5_p":  t5_prob_display,
        "t10_p": t10_prob_display,
        "t20_p": t20_prob_display,
        "c_p":   c_prob,
        # win odds
        "w_prob_is_live": w_prob_is_live,
        "w_dg_p": w_prob_display,    "w_dg":  fo_w.get("dg_odds"),
        "w_dk":   fo_w.get("draftkings"), "w_fd":  fo_w.get("fanduel"),
        "w_mgm":  fo_w.get("betmgm"),     "w_czr": fo_w.get("caesars"),
        "w_365":  fo_w.get("bet365"),     "w_score": fo_w.get("thescore"),
        "w_hr":   fo_w.get("hardrock"),   "w_best":w_best_odds,
        "w_bk":   fo_w.get("best_book"),
        # top 5 odds
        "t5_dk":  fo_5.get("draftkings"), "t5_fd": fo_5.get("fanduel"),
        "t5_best":t5_best_odds,           "t5_bk": fo_5.get("best_book"),
        # top 10 odds
        "t10_dk": fo_10.get("draftkings"),"t10_fd":fo_10.get("fanduel"),
        "t10_best":t10_best_odds,         "t10_bk":fo_10.get("best_book"),
        # top 20 odds
        "t20_best":implied_from_best(fo_20),"t20_bk":fo_20.get("best_book"),
        # cut odds
        "c_best":  implied_from_best(fo_c), "c_bk":  fo_c.get("best_book"),
        "course_rounds": course_rounds.get(did, []),
    })
field_players.sort(key=lambda x: (x["dg_rank"] or 9999))

# ── Metric cards ─────────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f"""<div class="metric-card">
        <div class="label">Current Event</div>
        <div class="value" style="font-size:1.1rem">{current_event}</div>
    </div>""", unsafe_allow_html=True)
with c2:
    st.markdown(f"""<div class="metric-card">
        <div class="label">Field Size</div>
        <div class="value">{len(field_players)}</div>
        <div class="sub">active players</div>
    </div>""", unsafe_allow_html=True)
with c3:
    # Count edges across all markets
    pos_edges = 0
    for p in field_players:
        for prob_key, odds_key, market in [
            ("w_dg_p", "w_best", "win"),
            ("t5_p", "t5_best", "top_5"),
            ("t10_p", "t10_best", "top_10"),
            ("c_p", "c_best", "make_cut"),
        ]:
            e = edge_pct(p.get(prob_key), p.get(odds_key))
            threshold = SHARP_THRESHOLDS.get(market, 2.0)
            if e and e >= threshold:
                pos_edges += 1
                break  # count player once even if multiple markets have edge
    st.markdown(f"""<div class="metric-card">
        <div class="label">Sharp Plays</div>
        <div class="value">{pos_edges}</div>
        <div class="sub">edges above threshold</div>
    </div>""", unsafe_allow_html=True)
with c4:
    h2h_sharp = sum(
        1 for m in matchups
        for side, dg_key, bk_keys in [
            ("p1", "p1_dg_odds", ["p1_best_odds","p1_draftkings","p1_fanduel","p1_betmgm","p1_caesars","p1_bet365","p1_thescore","p1_hardrock"]),
            ("p2", "p2_dg_odds", ["p2_best_odds","p2_draftkings","p2_fanduel","p2_betmgm","p2_caesars","p2_bet365","p2_thescore","p2_hardrock"]),
        ]
        for bk_odds in [[m.get(k) for k in bk_keys if m.get(k)]]
        if bk_odds and edge_pct(
            american_to_implied(m.get(dg_key)),
            bk_odds[0]
        ) and (edge_pct(american_to_implied(m.get(dg_key)), bk_odds[0]) or 0) >= SHARP_THRESHOLDS["matchup"]
    ) if matchups else 0
    st.markdown(f"""<div class="metric-card">
        <div class="label">Last Synced</div>
        <div class="value" style="font-size:1rem">{now_et()}</div>
        <div class="sub">{(datetime.now(timezone.utc) - timedelta(hours=4)).strftime("%b %d, %Y")} · {h2h_sharp} H2H sharp plays</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Tabs ─────────────────────────────────────────────────────────────────────────
tab_forecast, tab_edges, tab_h2h, tab_live, tab_alerts, tab_tracker, tab_research = st.tabs([
    "📊 Forecast",
    "💰 Edges",
    "⚔️ H2H",
    "🔴 Live",
    "🚨 Alerts",
    "📝 Tracker",
    "🏌️ Research",
])

# ════════════════════════════════════════════════════════════
# VIEW: TOURNAMENT FORECAST
# ════════════════════════════════════════════════════════════
def _render_tournament_forecast():
    st.markdown('<div class="section-header">Tournament Win / Finish Probabilities</div>', unsafe_allow_html=True)

    col_filter, col_sort, col_min = st.columns([2, 2, 2])
    with col_filter:
        search = st.text_input("🔍 Filter player", placeholder="Search name...", label_visibility="collapsed")
    with col_sort:
        sort_by = st.selectbox("Sort by", ["DG Rank", "Win Prob", "Win Edge", "Top 5%", "Top 10%", "Course Win%"], label_visibility="collapsed")
    with col_min:
        min_edge = st.slider("Min Win Edge %", -10.0, 10.0, -10.0, 0.5, label_visibility="collapsed")

    rows = []
    for p in field_players:
        if search and search.lower() not in p["name"].lower():
            continue
        e_w = edge_pct(p["w_dg_p"], p["w_best"])
        if e_w is None: e_w = 0
        # Suppress win edge if player has no live prob (still on pre-tournament baseline)
        if not p.get("w_prob_is_live"):
            e_w = 0
        if e_w < min_edge:
            continue
        ew,  sharp_w  = sharp_value(p["w_dg_p"] if p.get("w_prob_is_live") else None, p["w_best"],  "win")
        e5,  sharp_5  = sharp_value(p["t5_p"],   p["t5_best"], "top_5")
        e10, sharp_10 = sharp_value(p["t10_p"],  p["t10_best"],"top_10")
        ec,  sharp_c  = sharp_value(p["c_p"],    p["c_best"],  "make_cut")
        # Best sharp flag across all markets
        sharp_label = sharp_w or sharp_5 or sharp_10 or sharp_c or "—"
        best_market = ""
        best_edge   = None
        if sharp_w:    best_market, best_edge = "Win",    ew
        elif sharp_5:  best_market, best_edge = "Top 5",  e5
        elif sharp_10: best_market, best_edge = "Top 10", e10
        elif sharp_c:  best_market, best_edge = "Cut",    ec
        sharp_display = f"{sharp_label} {best_market} +{best_edge:.2f}%" if best_edge else sharp_label
        rows.append({
            "Player":        p["name"],
            "DG Rank":       p["dg_rank"] or "NR",
            "SG Total":      round(p["sg_total"], 3) if p["sg_total"] else None,
            "Win% (Base)":   round(p["bl_win"] or 0, 2),
            "Win% (Course)": round(p["co_win"] or 0, 2),
            "Top 5%":        round(p["bl_top5"] or 0, 2),
            "Top 10%":       round(p["bl_top10"] or 0, 2),
            "Make Cut%":     round(p["bl_cut"] or 0, 2),
            "DG Odds (Win)": fmt_odds(p["w_dg"]),
            "Best Odds":     fmt_odds(p["w_best"]),
            "Best Book":     (p["w_bk"] or "").title(),
            "Win Edge%":     round(e_w, 2),
            "Sharp Value":   sharp_display,
        })

    df1 = pd.DataFrame(rows)
    if not df1.empty:
        sort_map = {
            "DG Rank":      "DG Rank",
            "Win Prob":     "Win% (Base)",
            "Win Edge":     "Win Edge%",
            "Top 5%":       "Top 5%",
            "Top 10%":      "Top 10%",
            "Course Win%":  "Win% (Course)",
        }
        asc = sort_by == "DG Rank"
        df1 = df1.sort_values(
            sort_map[sort_by],
            ascending=asc,
            key=lambda col: col.map(lambda x: 9999 if x == "NR" else (x if isinstance(x, (int,float)) else 9999))
            if sort_map[sort_by] == "DG Rank" else col
        )

        def color_edge(val):
            if isinstance(val, (int, float)):
                if val > 2: return "background-color:#1b3a1b; color:#69f0ae; font-weight:600"
                if val < -2: return "background-color:#3a1b1b; color:#ef9a9a"
            return ""

        def color_sharp(val):
            if isinstance(val, str):
                if "STRONG" in val: return "background-color:#1a3a1a; color:#69f0ae; font-weight:700"
                if "SHARP"  in val: return "background-color:#1e3320; color:#a5d6a7; font-weight:600"
                if "VALUE"  in val: return "background-color:#1b2e1b; color:#81c784"
            return ""

        styled = df1.style\
            .map(color_edge, subset=["Win Edge%"])\
            .map(color_sharp, subset=["Sharp Value"])\
            .format({
                "Win% (Base)":   "{:.2f}",
                "Win% (Course)": "{:.2f}",
                "Top 5%":        "{:.2f}",
                "Top 10%":       "{:.2f}",
                "Make Cut%":     "{:.2f}",
                "Win Edge%":     "{:+.2f}",
                "SG Total":      "{:+.3f}",
            }, na_rep="—")

        st.dataframe(styled, use_container_width=True, hide_index=True, height=500)

        st.markdown(f"""<div class="info-box">
            🟢 <b>Green edge</b> = model win probability exceeds book implied probability by &gt;2% — potential value bet<br>
            📊 Showing {len(df1)} players · Sorted by <b>{sort_by}</b>
        </div>""", unsafe_allow_html=True)
    else:
        st.info("No players match the current filters.")

# ════════════════════════════════════════════════════════════
# VIEW: FINISH ODDS + EDGE
# ════════════════════════════════════════════════════════════
def _render_finish_odds():
    st.markdown('<div class="section-header">Finish Position Odds — Best Available Across All Books</div>', unsafe_allow_html=True)

    market_sel = st.radio("Market", ["Win", "Top 5", "Top 10", "Top 20"],
                          horizontal=True, label_visibility="collapsed")

    market_key_map = {"Win":"win","Top 5":"top_5","Top 10":"top_10","Top 20":"top_20","Make Cut":"make_cut"}
    mk_threshold = market_key_map[market_sel]
    market_map = {"Win":"w","Top 5":"t5","Top 10":"t10","Top 20":"t20","Make Cut":"c"}
    mk = market_map[market_sel]
    prob_key = f"{mk}_p"
    best_key = f"{mk}_best"
    bk_key   = f"{mk}_bk"
    dk_key   = f"{mk}_dk" if mk != "t20" and mk != "c" else None
    fd_key   = f"{mk}_fd" if mk != "t20" and mk != "c" else None

    if round_in_progress:
        st.markdown("""<div class="info-box">
            📊 <b>Round in progress</b> — book lines may be suspended during play.
            Values shown are <b>model predictions</b> (DG probability vs. last available price),
            not live actionable edges.
        </div>""", unsafe_allow_html=True)

    signal_col = "Model Signal" if round_in_progress else "Sharp Value"
    edge_col   = "Model%" if round_in_progress else "Edge%"

    rows2 = []
    for p in field_players:
        prob  = p.get(prob_key)
        best  = p.get(best_key)
        # For win market, suppress if player hasn't started (stale baseline)
        if mk == "w" and not p.get("w_prob_is_live"):
            e = 0.0
            sv_edge, sv = None, None
        else:
            e     = edge_pct(prob, best)
            sv_edge, sv = sharp_value(prob, best, mk_threshold)
        if round_in_progress and sv:
            sv = PRED_LABELS.get(sv, sv)
        sv_display = f"{sv} +{sv_edge:.2f}%" if sv and sv_edge else (sv or "—")
        row = {
            "Player":    p["name"],
            "DG Rank":   p["dg_rank"] or "NR",
            "DG Prob%":  round((prob or 0), 2),
            "Best Odds": fmt_odds(best),
            "Best Book": (p.get(bk_key) or "").title(),
            edge_col:    round(e, 2) if e is not None else 0.0,
            signal_col:  sv_display,
        }
        if dk_key:
            row["DraftKings"] = fmt_odds(p.get(dk_key))
        if fd_key:
            row["FanDuel"] = fmt_odds(p.get(fd_key))
        if mk == "w":
            row["BetMGM"]   = fmt_odds(p.get("w_mgm"))
            row["Caesars"]  = fmt_odds(p.get("w_czr"))
            row["Bet365"]   = fmt_odds(p.get("w_365"))
            row["theScore"] = fmt_odds(p.get("w_score"))
            row["Hard Rock"]= fmt_odds(p.get("w_hr"))
        rows2.append(row)

    df2 = pd.DataFrame(rows2).sort_values(edge_col, ascending=False)

    min_e2 = st.slider("Min%", -15.0, 15.0, -15.0, 0.5, label_visibility="collapsed")
    df2 = df2[df2[edge_col] >= min_e2]

    def color_edge2(val):
        if isinstance(val, (int, float)):
            if val > 3:  return "background-color:#1b3a1b; color:#69f0ae; font-weight:700"
            if val > 0:  return "background-color:#1e3320; color:#a5d6a7"
            if val < -3: return "background-color:#3a1b1b; color:#ef9a9a"
            if val < 0:  return "background-color:#2d1e1e; color:#ef9a9a"
        return ""

    def color_sharp2(val):
        if isinstance(val, str):
            if "STRONG" in val or "HIGH" in val: return "background-color:#1a3a1a; color:#69f0ae; font-weight:700"
            if "SHARP"  in val or "MED"  in val: return "background-color:#1e3320; color:#a5d6a7; font-weight:600"
            if "VALUE"  in val or "LOW"  in val: return "background-color:#1b2e1b; color:#81c784"
        return ""

    fmt_cols = {"DG Prob%": "{:.2f}%", edge_col: "{:+.2f}%"}
    styled2 = df2.style\
        .map(color_edge2, subset=[edge_col])\
        .map(color_sharp2, subset=[signal_col])\
        .format(fmt_cols, na_rep="—")
    st.dataframe(styled2, use_container_width=True, hide_index=True, height=400)

    # ── One-click Take It (only when not in live round) ──────────
    sharp_plays2 = [] if round_in_progress else [
        r for r in df2.to_dict("records")
        if r.get(signal_col) and "—" not in str(r.get(signal_col, ""))
    ]
    if sharp_plays2:
        st.markdown('<div class="section-header">🎯 Sharp Plays — Click to Log</div>', unsafe_allow_html=True)
        for i, play in enumerate(sharp_plays2):
            edge    = play.get(edge_col, 0)
            sv      = play.get(signal_col, "")
            player  = play.get("Player","")
            best    = play.get("Best Odds","—")
            book    = play.get("Best Book","")
            prob    = play.get("DG Prob%", 0)
            border  = "#69f0ae" if edge >= 5 else ("#a5d6a7" if edge >= 3 else "#81c784")

            col_info, col_btn = st.columns([5, 1])
            with col_info:
                st.markdown(f"""
                <div style="border-left:4px solid {border}; padding:10px 14px; margin:4px 0;
                            background:#1a1a1a; border-radius:4px;">
                    <span style="color:{border}; font-weight:700">{sv}</span>
                    <span style="color:#fff; font-weight:600; margin-left:12px">{player}</span>
                    <span style="color:#888"> · {market_sel}</span>
                    <br>
                    <span style="color:#90a4ae; font-size:0.85rem">
                        DG Prob: {prob:.2f}% · Best Odds: {best} @ {book}
                    </span>
                </div>
                """, unsafe_allow_html=True)
            with col_btn:
                stake = st.session_state.get("default_stake", 10.0)
                if st.button("🎯 Take It", key=f"fo_take_{i}_{market_sel}"):
                    odds_val = None
                    try:
                        odds_val = int(str(best).replace("+",""))
                    except: pass
                    if odds_val:
                        ok = quick_log_bet(
                            player=player, market=market_sel,
                            book=book, odds=odds_val,
                            edge=edge, stake=stake,
                            event=current_event,
                            notes=f"DG Prob: {prob:.2f}%"
                        )
                        if ok:
                            st.success("✅ Logged!")
                            st.cache_data.clear()
                        else:
                            st.error("Failed")
                    else:
                        st.warning("No odds to log")

    pos = (df2[edge_col] > 2).sum()
    label = "model signals" if round_in_progress else "positive edges"
    st.markdown(f"""<div class="info-box">
        Market: <b>{market_sel}</b> · {len(df2)} players shown ·
        <span style="color:#69f0ae">{pos} {label} &gt;2%</span>
    </div>""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# VIEW: H2H MATCHUP TOOL
# ════════════════════════════════════════════════════════════
def _render_matchup_tool():
    st.markdown('<div class="section-header">Head-to-Head Player Comparison</div>', unsafe_allow_html=True)

    player_names = [p["name"] for p in field_players]
    col_p1, col_p2 = st.columns(2)
    with col_p1:
        p1_name = st.selectbox("Player 1", player_names, index=0, key="p1")
    with col_p2:
        p2_name = st.selectbox("Player 2", player_names,
                               index=1 if len(player_names) > 1 else 0, key="p2")

    p1 = next((p for p in field_players if p["name"] == p1_name), {})
    p2 = next((p for p in field_players if p["name"] == p2_name), {})

    # Player cards
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"""<div class="compare-card">
            <div class="player-name">{p1.get("name","—")}</div>
            <div class="player-rank">DG Rank #{p1.get("dg_rank","—")} · SG Total {f"{p1.get('sg_total',0):+.3f}"}</div>
        </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""<div class="compare-card">
            <div class="player-name">{p2.get("name","—")}</div>
            <div class="player-rank">DG Rank #{p2.get("dg_rank","—")} · SG Total {f"{p2.get('sg_total',0):+.3f}"}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # SG Comparison table
    st.markdown('<div class="section-header">Strokes Gained Profile</div>', unsafe_allow_html=True)
    sg_metrics = [
        ("SG: Total",    "sg_total", "+.3f"),
        ("SG: Off Tee",  "sg_ott",   "+.3f"),
        ("SG: Approach", "sg_app",   "+.3f"),
        ("SG: Around",   "sg_atg",   "+.3f"),
        ("SG: Putting",  "sg_putt",  "+.3f"),
        ("Win Prob%",    "bl_win",   ".2f"),
        ("Top 5%",       "bl_top5",  ".2f"),
        ("Top 10%",      "bl_top10", ".2f"),
        ("Make Cut%",    "bl_cut",   ".2f"),
    ]

    sg_rows = []
    for label, key, fmt in sg_metrics:
        v1 = p1.get(key)
        v2 = p2.get(key)
        edge = None
        if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
            edge = round(v1 - v2, 4)
        sg_rows.append({
            "Metric":   label,
            p1_name:    v1,
            p2_name:    v2,
            "P1 Edge":  edge,
        })

    df_sg = pd.DataFrame(sg_rows)

    def color_edge_h2h(val):
        if isinstance(val, (int, float)):
            if val > 0: return "color:#69f0ae; font-weight:600"
            if val < 0: return "color:#ef9a9a; font-weight:600"
        return ""

    styled_sg = df_sg.style\
        .map(color_edge_h2h, subset=["P1 Edge"])\
        .format({
            p1_name: lambda x: f"{x:+.3f}" if isinstance(x, float) else (f"{x:.2%}" if x and x < 1 else "—"),
            p2_name: lambda x: f"{x:+.3f}" if isinstance(x, float) else (f"{x:.2%}" if x and x < 1 else "—"),
            "P1 Edge": lambda x: f"{x:+.4f}" if isinstance(x, (int, float)) else "—",
        }, na_rep="—")
    st.dataframe(styled_sg, use_container_width=True, hide_index=True)

    # Course history
    st.markdown('<div class="section-header">Course History at This Event</div>', unsafe_allow_html=True)
    c1h, c2h = st.columns(2)
    for col, player in [(c1h, p1), (c2h, p2)]:
        with col:
            ch = sorted(player.get("course_rounds", []),
                        key=lambda x: (x.get("year",0), x.get("round_num",0)), reverse=True)
            if ch:
                ch_rows = [{
                    "Year":     r.get("year"),
                    "Rnd":      r.get("round_num"),
                    "Score":    r.get("score"),
                    "To Par":   (r.get("score") or 72) - 72,
                    "SG Tot":   r.get("sg_total"),
                    "SG OTT":   r.get("sg_ott"),
                    "SG APP":   r.get("sg_app"),
                    "SG Putt":  r.get("sg_putt"),
                } for r in ch[:8]]
                df_ch = pd.DataFrame(ch_rows)
                def tp_color(val):
                    if isinstance(val, (int,float)):
                        if val < 0: return "color:#69f0ae"
                        if val > 2: return "color:#ef9a9a"
                    return ""
                styled_ch = df_ch.style.map(tp_color, subset=["To Par"])\
                    .format({"To Par": "{:+d}", "SG Tot": "{:+.3f}", "SG OTT": "{:+.3f}",
                             "SG APP": "{:+.3f}", "SG Putt": "{:+.3f}"}, na_rep="—")
                st.caption(player.get("name",""))
                st.dataframe(styled_ch, use_container_width=True, hide_index=True)
            else:
                st.caption(player.get("name",""))
                st.info("No course history available.")

# ════════════════════════════════════════════════════════════
# VIEW: BEST H2H PLAYS
# ════════════════════════════════════════════════════════════
def _render_best_h2h():
    st.markdown('<div class="section-header">🎯 Best H2H Plays Today — Ranked by Edge</div>', unsafe_allow_html=True)

    st.markdown("""<div class="info-box">
        Matchups ranked by edge % (DG model win probability vs implied book probability).
        Only plays above the sharp threshold are shown. Refresh after running
        <code>py golf_sync.py --mode live</code> during tournament rounds.
    </div>""", unsafe_allow_html=True)

    col_f1, col_f2, col_f3 = st.columns([2, 2, 2])
    with col_f1:
        min_edge_h2h = st.slider("Min Edge %", 0.0, 10.0, 2.0, 0.5,
                                  key="h2h_edge", label_visibility="collapsed")
    with col_f2:
        round_filter = st.selectbox("Round", ["Round 4", "Round 3", "Round 2", "Round 1", "All Rounds"],
                                     label_visibility="collapsed")
    with col_f3:
        side_filter = st.selectbox("Side", ["Both Sides", "Favorites Only", "Underdogs Only"],
                                    label_visibility="collapsed")

    if matchups:
        h2h_rows = []
        for m in matchups:
            p1w = (m.get("p1_dg_win_prob") or 0) * 100
            p2w = (m.get("p2_dg_win_prob") or 0) * 100
            p1_dg_odds = m.get("p1_dg_odds")
            p2_dg_odds = m.get("p2_dg_odds")
            # Best available book line — only from approved books
            approved = {
                "DraftKings": m.get("p1_draftkings"), "FanDuel": m.get("p1_fanduel"),
                "BetMGM": m.get("p1_betmgm"), "Caesars": m.get("p1_caesars"),
                "Bet365": m.get("p1_bet365"), "theScore": m.get("p1_thescore"),
                "Hard Rock": m.get("p1_hardrock"),
            }
            p1_approved = {k: v for k, v in approved.items() if v}
            p1_best = max(p1_approved.values()) if p1_approved else None
            p1_best_bk = max(p1_approved, key=p1_approved.get) if p1_approved else ""

            approved2 = {
                "DraftKings": m.get("p2_draftkings"), "FanDuel": m.get("p2_fanduel"),
                "BetMGM": m.get("p2_betmgm"), "Caesars": m.get("p2_caesars"),
                "Bet365": m.get("p2_bet365"), "theScore": m.get("p2_thescore"),
                "Hard Rock": m.get("p2_hardrock"),
            }
            p2_approved = {k: v for k, v in approved2.items() if v}
            p2_best = max(p2_approved.values()) if p2_approved else None
            p2_best_bk = max(p2_approved, key=p2_approved.get) if p2_approved else ""
            rnd = m.get("round_num", 0)

            # Round filter
            if round_filter != "All Rounds":
                rnd_num = int(round_filter.split()[-1])
                if rnd != rnd_num:
                    continue

            # DG model implied prob vs best book implied prob
            p1_dg_imp  = american_to_implied(p1_dg_odds) or 0
            p2_dg_imp  = american_to_implied(p2_dg_odds) or 0
            p1_bk_imp  = american_to_implied(p1_best) or 0
            p2_bk_imp  = american_to_implied(p2_best) or 0
            # Edge = DG model more likely than book implies
            p1_edge = round(p1_dg_imp - p1_bk_imp, 2) if p1_bk_imp and p1_dg_imp else None
            p2_edge = round(p2_dg_imp - p2_bk_imp, 2) if p2_bk_imp and p2_dg_imp else None

            # Sharp labels
            t = SHARP_THRESHOLDS["matchup"]
            def sv_label(edge):
                if edge is None: return "—"
                if edge >= t+3: return f"🔥🔥 STRONG +{edge:.2f}%"
                if edge >= t+1: return f"🔥 SHARP +{edge:.2f}%"
                if edge >= t:   return f"✅ VALUE +{edge:.2f}%"
                return f"{edge:+.2f}%"

            if p1_edge is not None and p1_edge >= min_edge_h2h:
                is_fav = p1_dg_odds and float(p1_dg_odds) < 0
                if side_filter == "Favorites Only" and not is_fav: pass
                elif side_filter == "Underdogs Only" and is_fav: pass
                else:
                    h2h_rows.append({
                        "Round":      rnd,
                        "Bet On":     m.get("p1_name",""),
                        "Opponent":   m.get("p2_name",""),
                        "DG Win%":    round(p1_dg_imp, 2),
                        "Book Impl%": round(p1_bk_imp, 2),
                        "DG Odds":    fmt_odds(p1_dg_odds),
                        "Best Book":  fmt_odds(p1_best),
                        "Book":       p1_best_bk.title(),
                        "Edge%":      p1_edge,
                        "Sharp Value":sv_label(p1_edge),
                    })

            if p2_edge is not None and p2_edge >= min_edge_h2h:
                is_fav = p2_dg_odds and float(p2_dg_odds) < 0
                if side_filter == "Favorites Only" and not is_fav: pass
                elif side_filter == "Underdogs Only" and is_fav: pass
                else:
                    h2h_rows.append({
                        "Round":      rnd,
                        "Bet On":     m.get("p2_name",""),
                        "Opponent":   m.get("p1_name",""),
                        "DG Win%":    round(p2_dg_imp, 2),
                        "Book Impl%": round(p2_bk_imp, 2),
                        "DG Odds":    fmt_odds(p2_dg_odds),
                        "Best Book":  fmt_odds(p2_best),
                        "Book":       p2_best_bk.title(),
                        "Edge%":      p2_edge,
                        "Sharp Value":sv_label(p2_edge),
                    })

        if h2h_rows:
            sorted_plays = sorted(h2h_rows, key=lambda x: -x["Edge%"])
            st.markdown(f"""<div class="info-box">
                {len(sorted_plays)} plays above {min_edge_h2h:.1f}% edge threshold ·
                Sorted by edge % · Click <b>🎯 Take It</b> to instantly log the bet
            </div>""", unsafe_allow_html=True)

            for i, play in enumerate(sorted_plays):
                edge   = play["Edge%"]
                sv     = play["Sharp Value"]
                player = play["Bet On"]
                opp    = play["Opponent"]
                dg_odds= play["DG Odds"]
                best   = play["Best Book"]
                book   = play.get("Book","")
                dg_w   = play["DG Win%"]
                bk_w   = play["Book Impl%"]

                # Color border by tier
                if edge >= 5:   border = "#69f0ae"
                elif edge >= 3: border = "#a5d6a7"
                else:           border = "#81c784"

                col_info, col_btn = st.columns([5, 1])
                with col_info:
                    st.markdown(f"""
                    <div style="border-left:4px solid {border}; padding:10px 14px; margin:4px 0;
                                background:#1a1a1a; border-radius:4px;">
                        <span style="color:{border}; font-weight:700; font-size:1rem">{sv}</span>
                        <span style="color:#fff; font-weight:600; margin-left:12px">{player}</span>
                        <span style="color:#888"> vs {opp}</span>
                        <span style="color:#ffcc02; margin-left:16px">R{play['Round']}</span>
                        <br>
                        <span style="color:#90a4ae; font-size:0.85rem">
                            DG Model: {dg_w:.1f}% · Book Implied: {bk_w:.1f}% ·
                            DG Odds: {dg_odds} · Best Book: {best} ({book})
                        </span>
                    </div>
                    """, unsafe_allow_html=True)
                with col_btn:
                    stake = st.session_state.get("default_stake", 10.0)
                    if st.button(f"🎯 Take It", key=f"h2h_take_{i}"):
                        odds_val = None
                        for odds_str in [best, dg_odds]:
                            if odds_str and odds_str not in ("—", "None", ""):
                                try:
                                    odds_val = int(str(odds_str).replace("+",""))
                                    break
                                except: pass
                        if odds_val:
                            ok = quick_log_bet(
                                player=player, market="H2H",
                                book=book or "Best Available",
                                odds=odds_val, edge=edge,
                                stake=stake, event=current_event,
                                notes=f"vs {opp} | DG: {dg_w:.1f}% | Book: {bk_w:.1f}%"
                            )
                            if ok:
                                st.success(f"✅ Logged!")
                                st.cache_data.clear()
                            else:
                                st.error("Failed — check bets table exists")
                        else:
                            st.warning("No odds available to log")
        else:
            st.markdown("""<div class="info-box">
                No plays meet the current edge threshold. Try lowering the Min Edge % slider
                or run <code>py golf_sync.py --mode live</code> to refresh matchup odds.
            </div>""", unsafe_allow_html=True)
    else:
        st.markdown("""<div class="info-box">
            ⏳ Round matchup odds haven't been released yet for this round.<br>
            Run <code>py golf_sync.py --mode live</code> once pairings are posted.
        </div>""", unsafe_allow_html=True)




# ════════════════════════════════════════════════════════════
# VIEW: LIVE LEADERBOARD
# ════════════════════════════════════════════════════════════
def _render_leaderboard():
    st.markdown('<div class="section-header">🏆 Live Leaderboard — Masters Tournament</div>', unsafe_allow_html=True)

    @st.cache_data(ttl=60)
    def load_live():
        sb = get_supabase()
        return sb.table("live_predictions").select("*").order("current_pos").execute().data

    live = load_live()

    if live and any(p.get("current_pos") for p in live):
        st.markdown("""<div class="info-box">
            Live scoring updates every 60 seconds · Win% = DG in-play model probability ·
            Run <code>py golf_sync.py --mode live</code> to push latest scores
        </div>""", unsafe_allow_html=True)

        lb_rows = []
        for p in live:
            pos   = p.get("current_pos")
            score = p.get("current_score")
            thru  = p.get("thru")
            win   = american_to_implied(p.get("win_prob")) or 0
            t5    = american_to_implied(p.get("top5_prob")) or 0
            t10   = american_to_implied(p.get("top10_prob")) or 0
            cut   = american_to_implied(p.get("make_cut_prob")) or 0

            # Find player's pre-tournament win prob for comparison
            fp = next((x for x in field_players if x["name"] == p.get("player_name")), {})
            pre_win = fp.get("bl_win") or 0

            lb_rows.append({
                "Pos":        pos or "—",
                "Player":     p.get("player_name",""),
                "Score":      score,
                "Thru":       thru if thru and thru < 18 else "F",
                "Win%":       round(win, 2),
                "Pre Win%":   round(pre_win, 2),
                "Win Δ":      round(win - pre_win, 2),
                "Top 5%":     round(t5, 2),
                "Top 10%":    round(t10, 2),
                "Make Cut%":  round(cut, 2),
            })

        df_lb = pd.DataFrame(lb_rows)

        def color_score(val):
            if isinstance(val, (int, float)):
                if val < 0: return "color:#69f0ae; font-weight:600"
                if val > 2: return "color:#ef9a9a"
            return ""
        def color_delta(val):
            if isinstance(val, (int, float)):
                if val > 2:  return "color:#69f0ae; font-weight:600"
                if val < -2: return "color:#ef9a9a"
            return ""

        styled_lb = df_lb.style\
            .map(color_score, subset=["Score"])\
            .map(color_delta, subset=["Win Δ"])\
            .format({
                "Score":     "{:+d}",
                "Win%":      "{:.2f}%",
                "Pre Win%":  "{:.2f}%",
                "Win Δ":     "{:+.2f}%",
                "Top 5%":    "{:.2f}%",
                "Top 10%":   "{:.2f}%",
                "Make Cut%": "{:.2f}%",
            }, na_rep="—")

        st.dataframe(styled_lb, use_container_width=True, hide_index=True, height=600)

        st.markdown("""<div class="info-box">
            <b>Win Δ</b> = Live win probability minus pre-tournament probability ·
            🟢 Green = model upgrading player · 🔴 Red = model downgrading
        </div>""", unsafe_allow_html=True)

        st.markdown("""<div class="info-box">
            🚨 Live bet alerts → see the <b>Alerts</b> tab
        </div>""", unsafe_allow_html=True)
    else:
        st.markdown("""<div class="info-box">
            ⏳ Live leaderboard populates once Round 1 begins Thursday morning.<br>
            Run <code>py golf_sync.py --mode live</code> during the round to push live scores.
            The leaderboard will show current position, score, holes completed, and
            updated win/top5/top10/cut probabilities from the DataGolf live model.
        </div>""", unsafe_allow_html=True)

        # Show pre-tournament order as preview
        st.markdown('<div class="section-header">Pre-Tournament Probability Order</div>', unsafe_allow_html=True)
        prev_rows = [{
            "Player":    p["name"],
            "DG Rank":   p["dg_rank"] or "NR",
            "Win%":      round(p["bl_win"] or 0, 2),
            "Top 5%":    round(p["bl_top5"] or 0, 2),
            "Top 10%":   round(p["bl_top10"] or 0, 2),
            "Make Cut%": round(p["bl_cut"] or 0, 2),
        } for p in field_players if p.get("bl_win")]
        df_prev = pd.DataFrame(prev_rows).sort_values("Win%", ascending=False)
        st.dataframe(df_prev.style.format({
            "Win%": "{:.2f}%", "Top 5%": "{:.2f}%",
            "Top 10%": "{:.2f}%", "Make Cut%": "{:.2f}%",
        }, na_rep="—"), use_container_width=True, hide_index=True, height=500)


# ════════════════════════════════════════════════════════════
# VIEW: RESULTS TRACKER
# ════════════════════════════════════════════════════════════
def _render_tracker():
    st.markdown('<div class="section-header">📝 Results Tracker — Bet Log, P&L & Learning Analytics</div>', unsafe_allow_html=True)

    @st.cache_data(ttl=30)
    def load_bets():
        try:
            sb = get_supabase()
            return sb.table("bets").select("*").order("logged_at", desc=True).execute().data
        except:
            return []

    bets = load_bets()
    settled  = [b for b in bets if b.get("result") not in ("Pending", None, "Void")]
    pending  = [b for b in bets if b.get("result") in ("Pending", None)]
    wins     = [b for b in settled if b.get("result") == "Win"]
    total_staked = sum(b.get("stake",0) or 0 for b in bets)
    total_pl     = sum(b.get("profit_loss",0) or 0 for b in settled)
    roi          = round(total_pl / sum(b.get("stake",0) or 0 for b in settled) * 100, 1) if settled else 0
    win_rate     = round(len(wins) / len(settled) * 100, 1) if settled else 0

    # ── Top metric cards ──────────────────────────────────────
    mc1, mc2, mc3, mc4, mc5, mc6 = st.columns(6)
    mc1.metric("Total Bets",    len(bets))
    mc2.metric("Settled",       len(settled))
    mc3.metric("Win Rate",      f"{win_rate:.1f}%")
    mc4.metric("Total Staked",  f"${total_staked:.2f}")
    mc5.metric("P&L",           f"${total_pl:+.2f}")
    mc6.metric("ROI",           f"{roi:+.1f}%")

    st.markdown("---")

    # ── Sub-tabs ──────────────────────────────────────────────
    r_tab1, r_tab2, r_tab3, r_tab4, r_tab5 = st.tabs([
        "➕ Log Bet", "📋 Bet Log", "📊 Learning Analytics",
        "🎯 Model Recommendations", "🏌️ Grade H2H",
    ])

    # ── LOG BET ───────────────────────────────────────────────
    with r_tab1:
        col_a, col_b, col_c = st.columns(3)
        with col_a:
            bet_player  = st.selectbox("Player", ["(Other)"] + [p["name"] for p in field_players], key="bt_player")
            bet_market  = st.selectbox("Market", ["Win","Top 5","Top 10","Top 20","Make Cut","H2H"], key="bt_market")
            bet_side    = st.text_input("Side / Description", placeholder="e.g. Scheffler to Win", key="bt_side")
            bet_tournament = st.text_input("Tournament", value=current_event, key="bt_tourn")
        with col_b:
            bet_book    = st.selectbox("Book", ["DraftKings","FanDuel","BetMGM","Caesars","Bet365","Hard Rock","theScore"], key="bt_book")
            bet_odds    = st.number_input("Odds (American)", value=-110, step=5, key="bt_odds")
            bet_stake   = st.number_input("Stake ($)", value=10.0, step=5.0, key="bt_stake")
            bet_closing = st.number_input("Closing Line Odds (optional)", value=0, step=5, key="bt_closing",
                                          help="Enter the closing line odds to track CLV. Leave 0 if unknown.")
        with col_c:
            bet_edge    = st.number_input("Model Edge % at bet", value=0.0, step=0.1, key="bt_edge")
            bet_round   = st.selectbox("Round", ["Pre-Tournament","R1","R2","R3","R4"], key="bt_round")
            bet_thresh  = st.selectbox("Edge Tier at Bet",
                ["VALUE (2-3%)", "SHARP (3-5%)", "STRONG (5%+)", "Below Threshold", "Manual Pick"],
                key="bt_thresh")
            bet_notes   = st.text_area("Notes", placeholder="Why this bet? What does the model say?", height=80, key="bt_notes")

        if st.button("➕ Log Bet", key="log_btn", type="primary"):
            try:
                sb = get_supabase()
                odds    = float(bet_odds)
                imp     = round((100/(odds+100)*100) if odds > 0 else (abs(odds)/(abs(odds)+100)*100), 2)
                win_amt = round((bet_stake * odds / 100) if odds > 0 else (bet_stake * 100 / abs(odds)), 2)
                clv     = None
                if bet_closing and float(bet_closing) != 0:
                    cl_imp = (100/(float(bet_closing)+100)*100) if float(bet_closing) > 0 else (abs(float(bet_closing))/(abs(float(bet_closing))+100)*100)
                    clv    = round(imp - cl_imp, 2)  # positive = beat the closing line
                row = {
                    "player_name":  bet_player if bet_player != "(Other)" else bet_side,
                    "market":       bet_market,
                    "side":         bet_side or bet_player,
                    "book":         bet_book,
                    "odds":         int(bet_odds),
                    "stake":        float(bet_stake),
                    "to_win":       win_amt,
                    "implied_prob": imp,
                    "edge_at_bet":  float(bet_edge),
                    "round":        bet_round,
                    "notes":        f"[{bet_thresh}] [{bet_tournament}] {bet_notes}",
                    "result":       "Pending",
                    "profit_loss":  0.0,
                    "logged_at":    (datetime.now(timezone.utc) - timedelta(hours=4)).isoformat(),
                }
                sb.table("bets").insert(row).execute()
                st.success(f"✅ Logged: {row['player_name']} {bet_market} @ {'+' if odds > 0 else ''}{int(odds)} | Edge: {bet_edge:+.1f}% | Stake: ${bet_stake:.2f}")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"Error: {e}")
                st.code("""CREATE TABLE IF NOT EXISTS bets (
    id bigserial primary key, player_name text, market text,
    side text, book text, odds integer, stake numeric,
    to_win numeric, implied_prob numeric, edge_at_bet numeric,
    round text, notes text, result text default 'Pending',
    profit_loss numeric default 0, logged_at timestamptz default now()
);""", language="sql")

    # ── BET LOG ───────────────────────────────────────────────
    with r_tab2:
        if not bets:
            st.info("No bets logged yet. Use the Log Bet tab to get started.")
        else:
            # Filter controls
            fc1, fc2, fc3 = st.columns(3)
            with fc1:
                filter_result = st.selectbox("Filter by Result",
                    ["All","Pending","Win","Loss","Push","Void"], key="filt_res")
            with fc2:
                filter_market = st.selectbox("Filter by Market",
                    ["All","Win","Top 5","Top 10","Top 20","Make Cut","H2H"], key="filt_mkt")
            with fc3:
                filter_book = st.selectbox("Filter by Book",
                    ["All","DraftKings","FanDuel","BetMGM","Caesars","Bet365","Hard Rock","theScore"], key="filt_bk")

            filt_bets = bets
            if filter_result != "All": filt_bets = [b for b in filt_bets if b.get("result") == filter_result]
            if filter_market != "All": filt_bets = [b for b in filt_bets if b.get("market") == filter_market]
            if filter_book   != "All": filt_bets = [b for b in filt_bets if b.get("book") == filter_book]

            bet_rows = []
            for b in filt_bets:
                odds = b.get("odds", 0)
                bet_rows.append({
                    "ID":       b.get("id"),
                    "Player":   b.get("player_name",""),
                    "Market":   b.get("market",""),
                    "Book":     b.get("book",""),
                    "Odds":     f"+{odds}" if odds > 0 else str(odds),
                    "Stake":    b.get("stake",0),
                    "To Win":   b.get("to_win",0),
                    "Edge%":    b.get("edge_at_bet",0) or 0,
                    "Round":    b.get("round",""),
                    "Result":   b.get("result","Pending"),
                    "P&L":      b.get("profit_loss",0) or 0,
                    "Notes":    (b.get("notes","") or "")[:40],
                })

            df_bets = pd.DataFrame(bet_rows)

            def color_result(val):
                if val == "Win":  return "color:#69f0ae; font-weight:700"
                if val == "Loss": return "color:#ef9a9a; font-weight:600"
                if val == "Push": return "color:#ffcc02"
                return "color:#90a4ae"
            def color_pl(val):
                if isinstance(val,(int,float)):
                    if val > 0: return "color:#69f0ae; font-weight:600"
                    if val < 0: return "color:#ef9a9a"
                return ""

            styled_bets = df_bets.drop(columns=["ID"]).style\
                .map(color_result, subset=["Result"])\
                .map(color_pl, subset=["P&L"])\
                .format({"Stake":"${:.2f}","To Win":"${:.2f}",
                         "Edge%":"{:+.1f}%","P&L":"${:+.2f}"}, na_rep="—")
            st.dataframe(styled_bets, use_container_width=True, hide_index=True, height=400)

            # ── Update result ──────────────────────────────────
            st.markdown('<div class="section-header">Update Result</div>', unsafe_allow_html=True)
            if bet_rows:
                uc1, uc2, uc3, uc4 = st.columns(4)
                with uc1:
                    upd_sel = st.selectbox("Bet",
                        [f"#{b['ID']} — {b['Player']} {b['Market']} {b['Odds']}" for b in bet_rows],
                        key="upd_sel")
                with uc2:
                    upd_result = st.selectbox("Result", ["Win","Loss","Push","Void"], key="upd_res")
                with uc3:
                    sel_id  = int(upd_sel.split("—")[0].replace("#","").strip())
                    sel_bet = next((b for b in bets if b.get("id") == sel_id), {})
                    if upd_result == "Win":   def_pl = float(sel_bet.get("to_win",0))
                    elif upd_result == "Loss":def_pl = -float(sel_bet.get("stake",0))
                    else:                     def_pl = 0.0
                    upd_pl = st.number_input("P&L ($)", value=def_pl, step=1.0, key="upd_pl2")
                with uc4:
                    st.markdown("<br>", unsafe_allow_html=True)
                    if st.button("✅ Update", key="upd_btn2"):
                        try:
                            sb = get_supabase()
                            sb.table("bets").update({"result":upd_result,"profit_loss":float(upd_pl)}).eq("id",sel_id).execute()
                            st.success(f"Updated #{sel_id} → {upd_result} ${upd_pl:+.2f}")
                            st.cache_data.clear()
                        except Exception as e:
                            st.error(f"Failed: {e}")

    # ── LEARNING ANALYTICS ────────────────────────────────────
    with r_tab3:
        if len(settled) < 3:
            st.info("Need at least 3 settled bets to show learning analytics. Log and settle some bets first.")
        else:
            st.markdown('<div class="section-header">📊 Edge Tier Performance — Are the sharp plays actually hitting?</div>', unsafe_allow_html=True)

            # Edge tier breakdown (uses module-level edge_tier)
            tier_stats = {}
            for b in settled:
                tier = edge_tier(b.get("edge_at_bet", 0))
                if tier not in tier_stats:
                    tier_stats[tier] = {"bets":0,"wins":0,"staked":0,"pl":0}
                tier_stats[tier]["bets"]   += 1
                tier_stats[tier]["wins"]   += 1 if b.get("result") == "Win" else 0
                tier_stats[tier]["staked"] += float(b.get("stake",0) or 0)
                tier_stats[tier]["pl"]     += float(b.get("profit_loss",0) or 0)

            tier_rows = []
            for tier, s in sorted(tier_stats.items(), key=lambda x: -x[1]["staked"]):
                wr = round(s["wins"]/s["bets"]*100,1) if s["bets"] else 0
                roi = round(s["pl"]/s["staked"]*100,1) if s["staked"] else 0
                tier_rows.append({
                    "Edge Tier":    tier,
                    "Bets":         s["bets"],
                    "Wins":         s["wins"],
                    "Win Rate":     wr,
                    "Staked":       s["staked"],
                    "P&L":          s["pl"],
                    "ROI":          roi,
                    "Verdict":      "✅ TAKE" if roi > 5 and wr > 30 else ("⚠️ WATCH" if roi > 0 else "❌ AVOID"),
                })

            df_tiers = pd.DataFrame(tier_rows)

            def color_verdict(val):
                if "TAKE"  in str(val): return "color:#69f0ae; font-weight:700"
                if "WATCH" in str(val): return "color:#ffcc02; font-weight:600"
                if "AVOID" in str(val): return "color:#ef9a9a; font-weight:600"
                return ""
            def color_roi(val):
                if isinstance(val,(int,float)):
                    if val > 5:  return "color:#69f0ae; font-weight:600"
                    if val > 0:  return "color:#a5d6a7"
                    return "color:#ef9a9a"
                return ""

            st.dataframe(df_tiers.style
                .map(color_verdict, subset=["Verdict"])
                .map(color_roi, subset=["ROI"])
                .format({"Win Rate":"{:.1f}%","Staked":"${:.2f}","P&L":"${:+.2f}","ROI":"{:+.1f}%"}, na_rep="—"),
                use_container_width=True, hide_index=True)

            st.markdown("---")
            st.markdown('<div class="section-header">📈 Market Performance — Which markets are profitable?</div>', unsafe_allow_html=True)

            mkt_stats = {}
            for b in settled:
                mkt = b.get("market","Unknown")
                if mkt not in mkt_stats:
                    mkt_stats[mkt] = {"bets":0,"wins":0,"staked":0,"pl":0}
                mkt_stats[mkt]["bets"]   += 1
                mkt_stats[mkt]["wins"]   += 1 if b.get("result") == "Win" else 0
                mkt_stats[mkt]["staked"] += float(b.get("stake",0) or 0)
                mkt_stats[mkt]["pl"]     += float(b.get("profit_loss",0) or 0)

            mkt_rows = []
            for mkt, s in sorted(mkt_stats.items(), key=lambda x: -x[1]["pl"]):
                wr  = round(s["wins"]/s["bets"]*100,1) if s["bets"] else 0
                roi = round(s["pl"]/s["staked"]*100,1) if s["staked"] else 0
                mkt_rows.append({
                    "Market":   mkt,
                    "Bets":     s["bets"],
                    "Win Rate": wr,
                    "Staked":   s["staked"],
                    "P&L":      s["pl"],
                    "ROI":      roi,
                })

            st.dataframe(pd.DataFrame(mkt_rows).style
                .map(color_roi, subset=["ROI"])
                .format({"Win Rate":"{:.1f}%","Staked":"${:.2f}","P&L":"${:+.2f}","ROI":"{:+.1f}%"}, na_rep="—"),
                use_container_width=True, hide_index=True)

            st.markdown("---")
            st.markdown('<div class="section-header">📚 Book Performance — Where are you getting best value?</div>', unsafe_allow_html=True)

            book_stats = {}
            for b in settled:
                book = b.get("book","Unknown")
                if book not in book_stats:
                    book_stats[book] = {"bets":0,"wins":0,"staked":0,"pl":0}
                book_stats[book]["bets"]   += 1
                book_stats[book]["wins"]   += 1 if b.get("result") == "Win" else 0
                book_stats[book]["staked"] += float(b.get("stake",0) or 0)
                book_stats[book]["pl"]     += float(b.get("profit_loss",0) or 0)

            book_rows = []
            for book, s in sorted(book_stats.items(), key=lambda x: -x[1]["pl"]):
                wr  = round(s["wins"]/s["bets"]*100,1) if s["bets"] else 0
                roi = round(s["pl"]/s["staked"]*100,1) if s["staked"] else 0
                book_rows.append({"Book":book,"Bets":s["bets"],"Win Rate":wr,
                                  "Staked":s["staked"],"P&L":s["pl"],"ROI":roi})
            st.dataframe(pd.DataFrame(book_rows).style
                .map(color_roi, subset=["ROI"])
                .format({"Win Rate":"{:.1f}%","Staked":"${:.2f}","P&L":"${:+.2f}","ROI":"{:+.1f}%"}, na_rep="—"),
                use_container_width=True, hide_index=True)

    # ── MODEL RECOMMENDATIONS ─────────────────────────────────
    with r_tab4:
        st.markdown('<div class="section-header">🎯 What Should You Take This Week?</div>', unsafe_allow_html=True)

        st.markdown("""<div class="info-box">
            Based on your historical results, here are the plays the model recommends
            taking this week. Recommendations update as you log and settle more bets —
            the system learns which edge tiers and markets are genuinely profitable for you.
        </div>""", unsafe_allow_html=True)

        # Build recommended plays from current field data
        rec_rows = []
        for p in field_players:
            for prob_key, odds_key, bk_key, market, mkt_label in [
                ("w_dg_p",  "w_best",   "w_bk",   "win",      "Win"),
                ("t5_p",    "t5_best",  "t5_bk",  "top_5",    "Top 5"),
                ("t10_p",   "t10_best", "t10_bk", "top_10",   "Top 10"),
                ("c_p",     "c_best",   "c_bk",   "make_cut", "Make Cut"),
            ]:
                prob = p.get(prob_key)
                odds = p.get(odds_key)
                if not prob or not odds: continue
                e, sv = sharp_value(prob, odds, market)
                if not sv: continue

                # Check if historical performance supports this tier
                tier  = edge_tier(e)
                hist  = tier_stats.get(tier, {}) if len(settled) >= 3 else {}
                hist_roi = round(hist.get("pl",0)/hist.get("staked",1)*100,1) if hist.get("staked") else None
                hist_note = f"Hist ROI: {hist_roi:+.1f}%" if hist_roi is not None else "No history yet"

                # Only recommend if no negative history
                if hist_roi is not None and hist_roi < -15:
                    continue

                rec_rows.append({
                    "Player":      p["name"],
                    "Market":      mkt_label,
                    "DG Prob%":    round(prob, 2),
                    "Best Odds":   fmt_odds(odds),
                    "Best Book":   (p.get(bk_key) or "").title(),
                    "Edge%":       round(e, 2),
                    "Sharp Value": sv,
                    "History":     hist_note,
                    "Confidence":  "🔥🔥 HIGH" if (hist_roi or 0) > 5 else ("🔥 MEDIUM" if sv else "✅ LOW"),
                })

        if rec_rows:
            df_rec = pd.DataFrame(rec_rows).sort_values("Edge%", ascending=False)

            def color_conf(val):
                if "HIGH"   in str(val): return "color:#69f0ae; font-weight:700"
                if "MEDIUM" in str(val): return "color:#ffcc02; font-weight:600"
                return "color:#90a4ae"

            st.dataframe(
                df_rec.style
                    .map(color_sharp, subset=["Sharp Value"])
                    .map(color_conf,  subset=["Confidence"])
                    .format({"DG Prob%":"{:.2f}%","Edge%":"{:+.2f}%"}, na_rep="—"),
                use_container_width=True, hide_index=True, height=350
            )

            st.markdown('<div class="section-header">🎯 Click to Log These Plays</div>', unsafe_allow_html=True)
            for i, play in enumerate(rec_rows):
                edge    = play["Edge%"]
                sv      = play["Sharp Value"]
                player  = play["Player"]
                market  = play["Market"]
                best    = play["Best Odds"]
                book    = play["Best Book"]
                prob    = play["DG Prob%"]
                conf    = play["Confidence"]
                hist    = play["History"]
                border  = "#69f0ae" if "HIGH" in conf else ("#ffcc02" if "MEDIUM" in conf else "#81c784")

                col_info, col_btn = st.columns([5, 1])
                with col_info:
                    st.markdown(f"""
                    <div style="border-left:4px solid {border}; padding:10px 14px; margin:4px 0;
                                background:#1a1a1a; border-radius:4px;">
                        <span style="color:{border}; font-weight:700">{sv}</span>
                        <span style="color:#fff; font-weight:600; margin-left:12px">{player}</span>
                        <span style="color:#888"> · {market}</span>
                        <span style="color:{border}; margin-left:12px; font-size:0.85rem">{conf}</span>
                        <br>
                        <span style="color:#90a4ae; font-size:0.85rem">
                            DG: {prob:.2f}% · Best: {best} @ {book} · {hist}
                        </span>
                    </div>
                    """, unsafe_allow_html=True)
                with col_btn:
                    stake = st.session_state.get("default_stake", 10.0)
                    if st.button("🎯 Take It", key=f"rec_take_{i}"):
                        odds_val = None
                        try:
                            odds_val = int(str(best).replace("+",""))
                        except: pass
                        if odds_val:
                            ok = quick_log_bet(
                                player=player, market=market,
                                book=book, odds=odds_val,
                                edge=edge, stake=stake,
                                event=current_event,
                                notes=f"Model rec | DG: {prob:.2f}% | {hist}"
                            )
                            if ok:
                                st.success("✅ Logged!")
                                st.cache_data.clear()
                            else:
                                st.error("Failed")
                        else:
                            st.warning("No odds available")

            st.markdown(f"""<div class="info-box">
                {len(df_rec)} recommended plays above threshold ·
                Confidence based on your historical ROI per edge tier ·
                System learns week over week as you log results
            </div>""", unsafe_allow_html=True)
        else:
            st.markdown("""<div class="info-box">
                No plays above threshold right now, or all edge tiers have negative historical ROI.
                Check the Finish Odds + Edge and Best H2H Plays tabs for current opportunities.
            </div>""", unsafe_allow_html=True)

    # ── GRADE H2H ─────────────────────────────────────────────
    with r_tab5:
        st.markdown('<div class="section-header">🏌️ Grade H2H Results</div>', unsafe_allow_html=True)
        st.caption("Auto-grades pending H2H bets using final scores from live_predictions. "
                   "Run a live sync first if scores look stale.")

        @st.cache_data(ttl=60)
        def load_h2h_scores():
            return (get_supabase()
                    .table("live_predictions")
                    .select("player_name,current_score,thru,current_pos")
                    .execute().data or [])

        final       = load_h2h_scores()
        score_map   = {
            p["player_name"]: {"score": p.get("current_score"),
                               "thru":  p.get("thru") or 0,
                               "pos":   p.get("current_pos")}
            for p in final if p.get("player_name")
        }

        h2h_pending  = [b for b in pending  if "H2H" in (b.get("market") or "")]
        h2h_settled  = [b for b in settled  if "H2H" in (b.get("market") or "")]
        all_h2h      = h2h_pending + h2h_settled

        if not all_h2h:
            st.info("No H2H bets logged yet. Use the Best H2H Plays tab to find and log plays.")
        else:
            # ─ Auto-grade pending ────────────────────────────────────────────────────
            if h2h_pending:
                st.markdown(f"**{len(h2h_pending)} pending H2H bet{'s' if len(h2h_pending) != 1 else ''}**")

                grade_rows = []
                for b in h2h_pending:
                    notes  = b.get("notes", "")
                    m      = re.search(r'vs ([^|]+)\|', notes)
                    opp    = m.group(1).strip() if m else None
                    player = b.get("player_name", "")

                    p_d    = score_map.get(player, {})
                    o_d    = score_map.get(opp, {}) if opp else {}
                    p_sc   = p_d.get("score")
                    o_sc   = o_d.get("score")
                    p_thru = p_d.get("thru", 0)
                    o_thru = o_d.get("thru", 0)
                    both_done = p_thru >= 18 and o_thru >= 18

                    if p_sc is not None and o_sc is not None:
                        if p_sc < o_sc:   raw = "Win"
                        elif p_sc > o_sc: raw = "Loss"
                        else:             raw = "Push"
                        proposed   = raw if both_done else raw + "*"
                        score_disp = f"{p_sc:+d} vs {o_sc:+d}"
                        data_ok    = True
                    else:
                        proposed   = "?"
                        score_disp = ("Score missing" if p_sc is None and o_sc is None
                                      else f"{'✓' if p_sc is not None else '?'} vs "
                                           f"{'✓' if o_sc is not None else '?'}")
                        data_ok    = False

                    stake  = float(b.get("stake") or 0)
                    to_win = float(b.get("to_win") or 0)
                    grade_rows.append({
                        "id": b["id"], "player": player, "opp": opp or "?",
                        "score_disp": score_disp, "edge": float(b.get("edge_at_bet") or 0),
                        "odds": b.get("odds") or 0, "stake": stake, "to_win": to_win,
                        "proposed": proposed, "data_ok": data_ok,
                    })

                def _gc(val):
                    if "Win"  in str(val) and "?" not in str(val): return "color:#69f0ae; font-weight:700"
                    if "Loss" in str(val): return "color:#ef9a9a; font-weight:700"
                    if "Push" in str(val): return "color:#ffcc02"
                    return "color:#90a4ae"

                st.dataframe(
                    pd.DataFrame([{
                        "Player":    r["player"],
                        "vs":        r["opp"],
                        "Scores":    r["score_disp"],
                        "Edge%":     r["edge"],
                        "Odds":      f"{'+' if r['odds'] > 0 else ''}{r['odds']}" if r["odds"] else "—",
                        "Stake":     f"${r['stake']:.0f}",
                        "Win P&L":   f"${r['to_win']:+.0f}",
                        "Loss P&L":  f"-${r['stake']:.0f}",
                        "Proposed":  r["proposed"],
                    } for r in grade_rows]).style
                        .map(_gc, subset=["Proposed"])
                        .format({"Edge%": "{:+.2f}%"}),
                    use_container_width=True, hide_index=True,
                )
                st.caption("* = provisional (one or both players not yet through 18)")

                confirmable = [r for r in grade_rows if r["data_ok"] and r["proposed"] != "?"]
                if confirmable:
                    if st.button(f"✅ Confirm {len(confirmable)} Grade{'s' if len(confirmable) != 1 else ''}",
                                 type="primary", key="h2h_grade_confirm"):
                        sb5 = get_supabase()
                        ok_count = 0
                        for r in confirmable:
                            result = r["proposed"].replace("*", "")
                            pl     = (r["to_win"] if result == "Win"
                                      else -r["stake"] if result == "Loss" else 0.0)
                            try:
                                sb5.table("bets").update({
                                    "result":      result,
                                    "profit_loss": round(pl, 2),
                                }).eq("id", r["id"]).execute()
                                ok_count += 1
                            except Exception as ge:
                                st.error(f"Failed #{r['id']}: {ge}")
                        if ok_count:
                            st.success(f"✅ Graded {ok_count} H2H bets!")
                            st.cache_data.clear()
                            st.rerun()
                else:
                    st.warning("Scores not found — run `py golf_sync.py --mode live` to refresh.")
            else:
                st.success("All H2H bets are settled. See theme analysis below.")

            st.markdown("---")

            # ─ Theme analysis ────────────────────────────────────────────────────────
            if len(h2h_settled) < 2:
                st.info(f"Grade at least 2 H2H bets to see theme analysis "
                        f"({len(h2h_settled)} settled so far).")
            else:
                st.markdown(f"### 🔍 Theme Analysis — {len(h2h_settled)} Settled H2H Bets")

                # Overall summary card
                tot_wins  = sum(1 for b in h2h_settled if b.get("result") == "Win")
                tot_pl    = sum(float(b.get("profit_loss") or 0) for b in h2h_settled)
                tot_stk   = sum(float(b.get("stake") or 0) for b in h2h_settled)
                tot_roi   = tot_pl / tot_stk * 100 if tot_stk else 0
                hit_rate  = tot_wins / len(h2h_settled) * 100
                sc        = "#69f0ae" if tot_pl > 0 else "#ef9a9a"
                st.markdown(f"""
                <div style="background:#1a1a1a; border:1px solid {sc}; border-radius:6px;
                            padding:12px 18px; margin-bottom:12px; display:flex; gap:28px">
                    <div><div style="color:#90a4ae;font-size:0.75rem">Bets</div>
                         <div style="color:#fff;font-weight:700;font-size:1.1rem">{len(h2h_settled)}</div></div>
                    <div><div style="color:#90a4ae;font-size:0.75rem">Hit Rate</div>
                         <div style="color:{sc};font-weight:700;font-size:1.1rem">{hit_rate:.0f}%</div></div>
                    <div><div style="color:#90a4ae;font-size:0.75rem">P&L</div>
                         <div style="color:{sc};font-weight:700;font-size:1.1rem">${tot_pl:+.2f}</div></div>
                    <div><div style="color:#90a4ae;font-size:0.75rem">ROI</div>
                         <div style="color:{sc};font-weight:700;font-size:1.1rem">{tot_roi:+.1f}%</div></div>
                </div>""", unsafe_allow_html=True)

                def _roi_c(val):
                    if isinstance(val, (int, float)):
                        return "color:#69f0ae; font-weight:600" if val > 0 else "color:#ef9a9a"
                    return ""

                th1, th2 = st.columns(2)

                # Edge tier performance
                with th1:
                    st.markdown("**By Edge Tier (3%+ plays)**")
                    tiers = {"3-5%": [], "5-8%": [], "8%+": []}
                    for b in h2h_settled:
                        edge = float(b.get("edge_at_bet") or 0)
                        if edge < 3: continue
                        bucket = "3-5%" if edge < 5 else ("5-8%" if edge < 8 else "8%+")
                        tiers[bucket].append(b)
                    tier_rows = []
                    for tier, tb in tiers.items():
                        if not tb: continue
                        w   = sum(1 for b in tb if b.get("result") == "Win")
                        pl  = sum(float(b.get("profit_loss") or 0) for b in tb)
                        stk = sum(float(b.get("stake") or 0) for b in tb)
                        tier_rows.append({
                            "Edge Tier": tier, "Bets": len(tb), "Wins": w,
                            "Hit %": f"{w/len(tb)*100:.0f}%",
                            "ROI":   round(pl/stk*100, 1) if stk else 0,
                        })
                    if tier_rows:
                        st.dataframe(
                            pd.DataFrame(tier_rows).style
                                .map(_roi_c, subset=["ROI"])
                                .format({"ROI": "{:+.1f}%"}),
                            use_container_width=True, hide_index=True,
                        )
                    else:
                        st.caption("No settled bets above 3% edge yet.")

                # Favorite vs underdog
                with th2:
                    st.markdown("**Favorite vs Underdog**")
                    favs, dogs = [], []
                    for b in h2h_settled:
                        odds = b.get("odds") or 0
                        if odds < 0:   favs.append(b)
                        elif odds > 0: dogs.append(b)
                    fv_rows = []
                    for label, grp in [("Favorite (−odds)", favs), ("Underdog (+odds)", dogs)]:
                        if not grp: continue
                        w   = sum(1 for b in grp if b.get("result") == "Win")
                        pl  = sum(float(b.get("profit_loss") or 0) for b in grp)
                        stk = sum(float(b.get("stake") or 0) for b in grp)
                        fv_rows.append({
                            "Side": label, "Bets": len(grp), "Wins": w,
                            "Hit %": f"{w/len(grp)*100:.0f}%",
                            "ROI":   round(pl/stk*100, 1) if stk else 0,
                        })
                    if fv_rows:
                        st.dataframe(
                            pd.DataFrame(fv_rows).style
                                .map(_roi_c, subset=["ROI"])
                                .format({"ROI": "{:+.1f}%"}),
                            use_container_width=True, hide_index=True,
                        )
                    else:
                        st.caption("No settled bets with odds data yet.")

                # Player-level breakdown
                st.markdown("**Player Performance — Who Crushed Their Matchups?**")
                plyr_stats = {}
                for b in h2h_settled:
                    p = b.get("player_name", "Unknown")
                    plyr_stats.setdefault(p, {"n":0,"wins":0,"pl":0.0,"stk":0.0})
                    plyr_stats[p]["n"]    += 1
                    plyr_stats[p]["wins"] += 1 if b.get("result") == "Win" else 0
                    plyr_stats[p]["pl"]   += float(b.get("profit_loss") or 0)
                    plyr_stats[p]["stk"]  += float(b.get("stake") or 0)

                plyr_rows = []
                for p, s in sorted(plyr_stats.items(), key=lambda x: -x[1]["pl"]):
                    plyr_rows.append({
                        "Player":   p,
                        "H2H Bets": s["n"],
                        "Wins":     s["wins"],
                        "Hit %":    f"{s['wins']/s['n']*100:.0f}%",
                        "P&L":      s["pl"],
                        "ROI":      round(s["pl"]/s["stk"]*100, 1) if s["stk"] else 0,
                    })

                def _pl_c(val):
                    if isinstance(val,(int,float)):
                        return "color:#69f0ae;font-weight:600" if val > 0 else "color:#ef9a9a"
                    return ""

                st.dataframe(
                    pd.DataFrame(plyr_rows).style
                        .map(_pl_c,  subset=["P&L"])
                        .map(_roi_c, subset=["ROI"])
                        .format({"P&L": "${:+.2f}", "ROI": "{:+.1f}%"}),
                    use_container_width=True, hide_index=True,
                )


# ════════════════════════════════════════════════════════════
# VIEW: SKILL RATINGS
# ════════════════════════════════════════════════════════════
def _render_skill_ratings():
    st.markdown('<div class="section-header">DataGolf Player Skill Ratings — All Ranked Players</div>', unsafe_allow_html=True)

    col_f, col_t = st.columns([3, 1])
    with col_f:
        search4 = st.text_input("🔍 Search player", placeholder="Filter by name...", label_visibility="collapsed", key="s4")
    with col_t:
        field_only = st.checkbox("This week's field only", value=False)

    sk_rows = []
    for p in skill:
        if search4 and search4.lower() not in (p.get("player_name","")).lower():
            continue
        if field_only and p["dg_id"] not in field_ids:
            continue
        sk_rows.append({
            "Rank":         p.get("dg_rank") or p.get("rank") or "NR",
            "Player":       p.get("player_name",""),
            "In Field":     "★" if p["dg_id"] in field_ids else "",
            "SG: Total":    p.get("sg_total"),
            "SG: OTT":      p.get("sg_ott"),
            "SG: APP":      p.get("sg_app"),
            "SG: ATG":      p.get("sg_atg"),
            "SG: Putt":     p.get("sg_putt"),
            "Drive Dist":   p.get("driving_dist"),
            "Drive Acc%":   p.get("driving_acc"),
        })

    df4 = pd.DataFrame(sk_rows)
    if not df4.empty:
        def sg_color(val):
            if isinstance(val, (int,float)):
                if val >  1.5: return "color:#69f0ae; font-weight:700"
                if val >  0.5: return "color:#a5d6a7"
                if val < -0.5: return "color:#ef9a9a"
            return ""

        styled4 = df4.style\
            .map(sg_color, subset=["SG: Total","SG: OTT","SG: APP","SG: ATG","SG: Putt"])\
            .format({
                "SG: Total": "{:+.3f}", "SG: OTT": "{:+.3f}",
                "SG: APP":   "{:+.3f}", "SG: ATG": "{:+.3f}",
                "SG: Putt":  "{:+.3f}", "Drive Dist": "{:.1f}",
                "Drive Acc%":"{:.1%}",
            }, na_rep="—")
        st.dataframe(styled4, use_container_width=True, hide_index=True, height=520)
        st.caption(f"{len(df4)} players shown")

# ════════════════════════════════════════════════════════════
# VIEW: COURSE HISTORY
# ════════════════════════════════════════════════════════════
def _render_course_history():
    st.markdown(f'<div class="section-header">Course History — {current_event}</div>', unsafe_allow_html=True)

    ch_rows = []
    for p in field_players:
        for r in p.get("course_rounds",[]):
            score = r.get("score")
            ch_rows.append({
                "Player":    p["name"],
                "DG Rank":   p["dg_rank"] or 9999,
                "Year":      r.get("year"),
                "Round":     r.get("round_num"),
                "Score":     score,
                "To Par":    (score - 72) if score else None,
                "SG: Tot":   r.get("sg_total"),
                "SG: OTT":   r.get("sg_ott"),
                "SG: APP":   r.get("sg_app"),
                "SG: ATG":   r.get("sg_atg"),
                "SG: Putt":  r.get("sg_putt"),
            })

    if ch_rows:
        df5 = pd.DataFrame(ch_rows)
        df5["_rank_sort"] = df5["DG Rank"].map(lambda x: 9999 if x == "NR" or x is None else x)
        df5 = df5.sort_values(["_rank_sort","Year","Round"]).drop(columns=["_rank_sort"])
        player_filter = st.multiselect("Filter players", sorted(df5["Player"].unique()), default=[])
        if player_filter:
            df5 = df5[df5["Player"].isin(player_filter)]

        def tp_col(val):
            if isinstance(val, (int,float)):
                if val < 0: return "color:#69f0ae"
                if val > 2: return "color:#ef9a9a"
            return ""
        def sg_col(val):
            if isinstance(val, (int,float)):
                if val > 1: return "color:#69f0ae"
                if val < -1: return "color:#ef9a9a"
            return ""

        styled5 = df5.style\
            .map(tp_col, subset=["To Par"])\
            .map(sg_col, subset=["SG: Tot"])\
            .format({
                "To Par":   "{:+d}", "SG: Tot": "{:+.3f}",
                "SG: OTT":  "{:+.3f}", "SG: APP": "{:+.3f}",
                "SG: ATG":  "{:+.3f}", "SG: Putt": "{:+.3f}",
            }, na_rep="—")
        st.dataframe(styled5, use_container_width=True, hide_index=True, height=520)
        st.caption(f"{len(df5)} rounds shown")
    else:
        st.info("Course history will populate as rounds are completed during the tournament.")

# ════════════════════════════════════════════════════════════
# VIEW: LIVE MATCHUPS
# ════════════════════════════════════════════════════════════
def _render_live_matchups():
    st.markdown('<div class="section-header">Live Round H2H Matchup Odds — DataGolf Model</div>', unsafe_allow_html=True)

    if matchups:
        m_rows = []
        for m in matchups:
            p1w = (m.get("p1_dg_win_prob") or 0) * 100
            p2w = (m.get("p2_dg_win_prob") or 0) * 100
            p1_odds = m.get("p1_dg_odds")
            p2_odds = m.get("p2_dg_odds")
            # Sharp value: flag favorites where DG win prob diverges from odds
            p1_imp = american_to_implied(p1_odds)
            p2_imp = american_to_implied(p2_odds)
            p1_edge = round(p1w - p1_imp, 1) if p1_imp and p1w else None
            p2_edge = round(p2w - p2_imp, 1) if p2_imp and p2w else None
            def matchup_sv(edge):
                if edge is None: return "—"
                t = SHARP_THRESHOLDS["matchup"]
                if edge >= t + 3: return f"🔥🔥 STRONG +{edge:.2f}%"
                if edge >= t + 1: return f"🔥 SHARP +{edge:.2f}%"
                if edge >= t:     return f"✅ VALUE +{edge:.2f}%"
                return "—"
            m_rows.append({
                "Round":        m.get("round_num"),
                "Player 1":     m.get("p1_name",""),
                "P1 Win%":      round(p1w, 1) if p1w else None,
                "P1 DG Odds":   fmt_odds(p1_odds),
                "P1 Sharp":     matchup_sv(p1_edge),
                "P2 Sharp":     matchup_sv(p2_edge),
                "P2 DG Odds":   fmt_odds(p2_odds),
                "P2 Win%":      round(p2w, 1) if p2w else None,
                "Player 2":     m.get("p2_name",""),
            })
        df6 = pd.DataFrame(m_rows)

        def fav_color(val):
            if isinstance(val, (int,float)) and val > 55:
                return "color:#69f0ae; font-weight:700"
            return ""
        def sharp_color(val):
            if isinstance(val, str):
                if "STRONG" in val: return "background-color:#1a3a1a; color:#69f0ae; font-weight:700"
                if "SHARP"  in val: return "background-color:#1e3320; color:#a5d6a7; font-weight:600"
                if "VALUE"  in val: return "background-color:#1b2e1b; color:#81c784"
            return ""
        styled6 = df6.style\
            .map(fav_color, subset=["P1 Win%","P2 Win%"])\
            .map(sharp_color, subset=["P1 Sharp","P2 Sharp"])\
            .format({"P1 Win%": "{:.1f}%", "P2 Win%": "{:.1f}%"}, na_rep="—")
        st.dataframe(styled6, use_container_width=True, hide_index=True, height=520)
        st.markdown(f"""<div class="info-box">
            {len(df6)} matchups loaded · Run <code>py golf_sync.py --mode live</code> to refresh
        </div>""", unsafe_allow_html=True)
    else:
        st.markdown("""<div class="info-box">
            ⏳ Round matchup odds are released Wed–Thu morning of tournament week.<br>
            Run <code>py golf_sync.py --mode live</code> to pull the latest lines when available.
        </div>""", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════
# VIEW: AUTO SCHEDULER
# ════════════════════════════════════════════════════════════
def _render_auto_scheduler():
    st.markdown('<div class="section-header">⚙️ Auto Scheduler — Windows Task Scheduler Setup</div>', unsafe_allow_html=True)

    st.markdown("""<div class="info-box">
        The auto scheduler runs <code>golf_sync.py --mode live</code> automatically during
        tournament rounds so you don't have to manually trigger syncs. Set it up once and
        it runs silently in the background all week.
    </div>""", unsafe_allow_html=True)

    st.markdown('<div class="section-header">Step 1 — Create the Batch File</div>', unsafe_allow_html=True)
    st.markdown("Save this as `golf_sync_live.bat` in your `C:\\Golf Model` folder:")
    st.code(r"""@echo off
cd /d "C:\Golf Model"
py golf_sync.py --mode live
""", language="batch")

    st.markdown('<div class="section-header">Step 2 — Open Task Scheduler</div>', unsafe_allow_html=True)
    st.markdown("""
1. Press **Win + S** and search **Task Scheduler**
2. Click **Create Basic Task** in the right panel
3. Name it: `Golf Model Live Sync`
4. Click **Next**
""")

    st.markdown('<div class="section-header">Step 3 — Set the Trigger</div>', unsafe_allow_html=True)
    st.markdown("""
1. Select **Daily** → Click **Next**
2. Set start time: **8:00 AM** (before Round 1 tee times)
3. Click **Next**
4. Select **Action: Start a program**
5. Browse to `C:\\Golf Model\\golf_sync_live.bat`
6. Click **Next** → **Finish**
""")

    st.markdown('<div class="section-header">Step 4 — Set Repeat Interval</div>', unsafe_allow_html=True)
    st.markdown("""
1. Find your new task in the Task Scheduler library
2. Right-click → **Properties**
3. Go to **Triggers** tab → **Edit**
4. Check **Repeat task every:** → set to **30 minutes**
5. Set **for a duration of:** → **12 hours**
6. Click **OK** → **OK**
""")

    st.markdown("""<div class="info-box">
        ✅ Once set up, the sync runs automatically every 30 minutes from 8AM to 8PM
        on tournament days. The Streamlit app auto-refreshes every 15 min and will
        pick up the latest data automatically.<br><br>
        💡 <b>Tip:</b> Only run the scheduler on tournament days (Thu–Sun).
        Use <code>--mode pre</code> manually on Monday/Tuesday of tournament week.
    </div>""", unsafe_allow_html=True)

    st.markdown('<div class="section-header">Manual Sync Commands</div>', unsafe_allow_html=True)
    col_s1, col_s2, col_s3 = st.columns(3)
    with col_s1:
        st.markdown("""**Pre-Tournament (Mon/Tue)**
```
py golf_sync.py --mode pre
```
Pulls field, predictions, finish odds, matchup odds""")
    with col_s2:
        st.markdown("""**Live (Thu–Sun during rounds)**
```
py golf_sync.py --mode live
```
Pulls live scores, updated probs, current odds""")
    with col_s3:
        st.markdown("""**Full Refresh (weekly)**
```
py golf_sync.py
```
Complete sync of all tables including historical data""")

# ════════════════════════════════════════════════════════════
# VIEW: BEST PLAYS BY BOOK
# ════════════════════════════════════════════════════════════
def _render_best_plays_by_book():
    st.markdown('<div class="section-header">📚 Best Plays by Book — Where to Bet Today</div>', unsafe_allow_html=True)
    st.markdown("""<div class="info-box">
        Top edges available at each sportsbook across all markets (Finish Position + H2H).
        Ranked by Sharp Value tier, then Edge %. Only plays ≥3% edge shown.
        Use this to know exactly which app to open for each bet.
    </div>""", unsafe_allow_html=True)

    APPROVED_BOOKS = [
        ("DraftKings",  "dk"),
        ("FanDuel",     "fd"),
        ("BetMGM",      "mgm"),
        ("Caesars",     "czr"),
        ("Bet365",      "365"),
        ("theScore",    "score"),
        ("Hard Rock",   "hr"),
    ]

    FINISH_MARKETS = [
        ("Win",      "w",   "win"),
        ("Top 5",    "t5",  "top_5"),
        ("Top 10",   "t10", "top_10"),
        ("Top 20",   "t20", "top_20"),
    ]

    # Map book label → finish_odds field key
    BOOK_FO_KEY = {
        "DraftKings": "w_dk",  "FanDuel": "w_fd",   "BetMGM": "w_mgm",
        "Caesars":    "w_czr", "Bet365":  "w_365",  "theScore": "w_score",
        "Hard Rock":  "w_hr",
    }
    BOOK_MK_PREFIX = {
        "DraftKings": "draftkings", "FanDuel": "fanduel",   "BetMGM": "betmgm",
        "Caesars":    "caesars",    "Bet365":  "bet365",    "theScore": "thescore",
        "Hard Rock":  "hardrock",
    }

    def tier_sort_key(tier, edge):
        order = {"🔥🔥 STRONG": 0, "🔥 SHARP": 1, "✅ VALUE": 2}
        t = tier.split(" +")[0] if tier else "—"
        return (order.get(t, 9), -(edge or 0))

    MIN_EDGE = 3.0
    book_plays = {book: [] for book, _ in APPROVED_BOOKS}

    # ── Finish position plays ──
    for market_label, mk, mk_threshold in FINISH_MARKETS:
        prob_key = f"{mk}_p"
        for p in field_players:
            prob = p.get(prob_key)
            if not prob: continue
            for book_name, _ in APPROVED_BOOKS:
                # Get this book's odds for this market
                if mk == "w":
                    book_field = f"w_{BOOK_MK_PREFIX[book_name]}" if mk == "w" else None
                    # use stored keys
                    bk_map = {
                        "DraftKings": p.get("w_dk"),  "FanDuel": p.get("w_fd"),
                        "BetMGM":     p.get("w_mgm"), "Caesars": p.get("w_czr"),
                        "Bet365":     p.get("w_365"), "theScore": p.get("w_score"),
                        "Hard Rock":  p.get("w_hr"),
                    }
                    odds = bk_map.get(book_name)
                else:
                    # For other markets pull from fo_index via field_players best key
                    # We only store best odds, not per-book for t5/t10/t20/cut
                    # Skip non-win per-book breakdown (not stored per book)
                    continue
                if not odds: continue
                # Win market: skip players who haven't started (stale baseline)
                if mk == "w" and not p.get("w_prob_is_live"):
                    continue
                e, sv = sharp_value(prob, odds, mk_threshold)
                if e is None or e < MIN_EDGE: continue
                if round_in_progress and sv:
                    sv = PRED_LABELS.get(sv, sv)
                sv_display = f"{sv} +{e:.2f}%" if sv else f"+{e:.2f}%"
                book_plays[book_name].append({
                    "tier":    sv.split(" +")[0] if sv else "—",
                    "edge":    e,
                    "label":   sv_display,
                    "play":    f"{p['name']} {market_label}",
                    "odds":    fmt_odds(odds),
                    "dg_prob": round(prob, 2),
                    "market":  market_label,
                })

    # ── H2H plays ──
    if matchups:
        # Find the highest round number available
        max_rnd = max((m.get("round_num") or 0) for m in matchups)
        for m in matchups:
            p1w = (m.get("p1_dg_win_prob") or 0) * 100
            p2w = (m.get("p2_dg_win_prob") or 0) * 100
            rnd = m.get("round_num", 0)
            # Only show current round in Best H2H Plays
            if rnd != max_rnd:
                continue
            rnd_label = f"R{rnd}" if rnd else ""
            for side, name_key, dg_prob, prefix in [
                ("p1", "p1_name", p1w, "p1"),
                ("p2", "p2_name", p2w, "p2"),
            ]:
                if not dg_prob: continue
                dg_odds = m.get(f"{prefix}_dg_odds")
                if not dg_odds: continue
                for book_name, _ in APPROVED_BOOKS:
                    bk = BOOK_MK_PREFIX[book_name]
                    odds = m.get(f"{prefix}_{bk}")
                    if not odds: continue
                    dg_imp = american_to_implied(dg_odds) or 0
                    bk_imp = american_to_implied(odds) or 0
                    e = round(dg_imp - bk_imp, 2)
                    if e < MIN_EDGE: continue
                    _, sv = sharp_value(dg_imp, odds, "matchup")
                    sv_display = f"{sv} +{e:.2f}%" if sv else f"+{e:.2f}%"
                    opp = m.get("p2_name" if side == "p1" else "p1_name", "")
                    book_plays[book_name].append({
                        "tier":    sv.split(" +")[0] if sv else "—",
                        "edge":    e,
                        "label":   sv_display,
                        "play":    f"{m.get(name_key,'')} vs {opp} {rnd_label}",
                        "odds":    fmt_odds(odds),
                        "dg_prob": round(dg_imp, 2),
                        "market":  f"H2H {rnd_label}",
                    })

    # ── Display — 2 columns of book cards ──
    total_plays = sum(len(v) for v in book_plays.values())
    if total_plays == 0:
        st.info("No plays above 3% edge threshold across any book. Run a live sync and refresh.")
    else:
        cols = st.columns(2)
        for i, (book_name, _) in enumerate(APPROVED_BOOKS):
            plays = sorted(book_plays[book_name], key=lambda x: tier_sort_key(x["tier"], x["edge"]))
            with cols[i % 2]:
                tier_counts = {}
                for pl in plays:
                    tier_counts[pl["tier"]] = tier_counts.get(pl["tier"], 0) + 1
                summary = " · ".join(f"{v}× {k}" for k, v in tier_counts.items())
                st.markdown(f"""
<div style="background:#1a1a1a;border:1px solid #333;border-radius:8px;padding:1rem;margin-bottom:1rem">
  <div style="font-size:1.1rem;font-weight:700;color:#fff;margin-bottom:0.4rem">
    {book_name}
    <span style="font-size:0.75rem;color:#aaa;font-weight:400;margin-left:0.5rem">{len(plays)} play{'s' if len(plays)!=1 else ''}{(' · ' + summary) if summary else ''}</span>
  </div>""", unsafe_allow_html=True)

                if not plays:
                    st.markdown('<div style="color:#666;font-size:0.85rem;padding:0.3rem 0">No plays above threshold</div>', unsafe_allow_html=True)
                else:
                    for pl in plays:
                        tier = pl["tier"]
                        color = "#69f0ae" if "STRONG" in tier else ("#a5d6a7" if "SHARP" in tier else "#81c784")
                        st.markdown(f"""
  <div style="border-left:3px solid {color};padding:0.4rem 0.6rem;margin:0.3rem 0;background:#222;border-radius:0 4px 4px 0">
    <span style="color:{color};font-weight:600;font-size:0.85rem">{pl['label']}</span>
    <span style="color:#fff;font-size:0.85rem;margin-left:0.5rem">{pl['play']}</span>
    <span style="color:#aaa;font-size:0.8rem;margin-left:0.5rem">· {pl['odds']} · DG {pl['dg_prob']:.1f}%</span>
  </div>""", unsafe_allow_html=True)

                st.markdown("</div>", unsafe_allow_html=True)

        st.markdown(f"""<div style="color:#aaa;font-size:0.8rem;margin-top:0.5rem">
            {total_plays} total plays above 3% edge · Refresh after running
            <code>py golf_sync.py --mode live</code>
        </div>""", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════
# LEARNING ENGINE — helpers for Alerts tab
# ════════════════════════════════════════════════════════════

_LE_BUCKETS = [
    (0,  25,  "0-25%"),
    (25, 35,  "25-35%"),
    (35, 45,  "35-45%"),
    (45, 55,  "45-55%"),
    (55, 65,  "55-65%"),
    (65, 101, "65%+"),
]
_LE_BUCKET_ORDER = [lb for *_, lb in _LE_BUCKETS]
_BASE_THRESHOLDS = {"Win": 20.0, "Top 5": 40.0, "Top 10": 55.0}


def _breakeven_odds(prob_pct):
    """American odds at which a bet breaks even given model probability."""
    if not prob_pct or prob_pct <= 0:
        return None
    p = prob_pct / 100
    if p >= 0.5:
        return int(round(-(p / (1 - p)) * 100))
    return int(round(((1 - p) / p) * 100))


def _compute_learning_engine(settled_bets):
    """
    Returns (calibration, mkt_roi, adaptive) from settled bet history.
    DG probability is reconstructed as implied_prob + edge_at_bet — both
    fields are written automatically by quick_log_bet.
    """
    # ─ Calibration by probability bucket ─────────────────────────────────────
    cd = {lb: {"psum": 0.0, "wins": 0, "n": 0} for *_, lb in _LE_BUCKETS}
    for b in settled_bets:
        dg = float(b.get("implied_prob") or 0) + float(b.get("edge_at_bet") or 0)
        if dg <= 0:
            continue
        is_win = b.get("result") == "Win"
        for lo, hi, lb in _LE_BUCKETS:
            if lo <= dg < hi:
                cd[lb]["psum"] += dg
                cd[lb]["wins"] += 1 if is_win else 0
                cd[lb]["n"]    += 1
                break

    calibration = {}
    for lb, s in cd.items():
        if s["n"] < 2:
            continue
        pred   = s["psum"] / s["n"]
        actual = s["wins"] / s["n"] * 100
        calibration[lb] = {
            "predicted": round(pred, 1),
            "actual":    round(actual, 1),
            "n":         s["n"],
            "ratio":     round(actual / pred, 3) if pred > 0 else 1.0,
            "diff":      round(actual - pred, 1),
        }

    # ─ Market ROI ─────────────────────────────────────────────────────────────
    ms = {}
    for b in settled_bets:
        mkt = b.get("market", "Unknown")
        ms.setdefault(mkt, {"n": 0, "wins": 0, "staked": 0.0, "pl": 0.0})
        ms[mkt]["n"]      += 1
        ms[mkt]["wins"]   += 1 if b.get("result") == "Win" else 0
        ms[mkt]["staked"] += float(b.get("stake") or 0)
        ms[mkt]["pl"]     += float(b.get("profit_loss") or 0)

    mkt_roi = {}
    for mkt, s in ms.items():
        roi = (s["pl"] / s["staked"] * 100) if s["staked"] else 0.0
        mkt_roi[mkt] = {
            "n":        s["n"],
            "roi":      round(roi, 1),
            "win_rate": round(s["wins"] / s["n"] * 100, 1) if s["n"] else 0.0,
            "pl":       round(s["pl"], 2),
            "staked":   round(s["staked"], 2),
        }

    # ─ Adaptive thresholds ────────────────────────────────────────────────────
    adaptive = {}
    for mkt, base in _BASE_THRESHOLDS.items():
        md = mkt_roi.get(mkt, {})
        n  = md.get("n", 0)
        if n < 5:
            adaptive[mkt] = {
                "threshold": base, "base": base, "delta": 0,
                "note": f"<5 bets settled — using base {base:.0f}%",
                "status": "neutral",
            }
            continue
        roi = md["roi"]
        if   roi < -15: delta, status = +10, "tight"
        elif roi < -5:  delta, status = +5,  "tight"
        elif roi < 0:   delta, status = +3,  "watch"
        elif roi > 20:  delta, status = -5,  "loose"
        elif roi > 10:  delta, status = -2,  "loose"
        else:           delta, status =  0,  "good"
        adaptive[mkt] = {
            "threshold": round(max(5.0, base + delta), 1),
            "base":      base,
            "delta":     delta,
            "note":      f"ROI {roi:+.1f}% over {n} bets",
            "status":    status,
        }

    return calibration, mkt_roi, adaptive


def _calib_factor(dg_prob, calibration):
    """Ratio of actual/predicted hit rate at this probability level (1.0 = perfectly calibrated)."""
    for lo, hi, lb in _LE_BUCKETS:
        if lo <= dg_prob < hi and lb in calibration:
            return calibration[lb]["ratio"]
    return 1.0


def _calib_note(dg_prob, calibration):
    """One-line calibration note for an alert card."""
    for lo, hi, lb in _LE_BUCKETS:
        if lo <= dg_prob < hi:
            if lb not in calibration:
                return f"{lb} bucket — no bet history yet"
            c     = calibration[lb]
            arrow = "↑" if c["diff"] >= 0 else "↓"
            perf  = ("overperforms" if c["diff"] > 2
                     else "on target" if abs(c["diff"]) <= 2
                     else "underperforms")
            return (f"{lb} bucket: DG predicts {c['predicted']:.0f}%, "
                    f"historically hits {c['actual']:.0f}% "
                    f"({arrow}{abs(c['diff']):.0f}pp — {perf}) · n={c['n']}")
    return "—"


def _confidence_score(dg_prob, market, calibration, mkt_roi):
    """
    0-100 composite confidence score.
    Base: DG probability (up to 75 pts)
    + Calibration adjustment: ±15 pts based on actual vs predicted hit rate
    + Market ROI adjustment: ±10 pts based on historical P&L in this market
    """
    base      = min(dg_prob * 0.9, 75.0)
    cf        = _calib_factor(dg_prob, calibration)
    calib_adj = max(-15.0, min(15.0, (cf - 1.0) * 20.0))
    md        = mkt_roi.get(market, {})
    mkt_adj   = max(-10.0, min(10.0, md["roi"] * 0.4)) if md.get("n", 0) >= 3 else 0.0
    return max(0, min(100, round(base + calib_adj + mkt_adj)))


def _kelly_stake(dg_prob_pct, book_odds_american, bankroll, fraction=0.5):
    """Half-Kelly recommended stake. Returns 0 if no edge, None if inputs missing."""
    if not book_odds_american or bankroll <= 0:
        return None
    p = dg_prob_pct / 100.0
    o = float(book_odds_american)
    b = (o / 100.0) if o > 0 else (100.0 / abs(o))   # profit per $1 staked
    kelly_f = (b * p - (1.0 - p)) / b
    if kelly_f <= 0:
        return 0.0
    return round(max(1.0, kelly_f * fraction * bankroll), 2)


# ════════════════════════════════════════════════════════════
# VIEW: LIVE ALERTS
# ════════════════════════════════════════════════════════════
def _render_live_alerts():
    # ─ Data ──────────────────────────────────────────────────────────────────────
    @st.cache_data(ttl=60)
    def load_alert_data():
        sb = get_supabase()
        live = sb.table("live_predictions").select("*").order("current_pos").execute().data or []
        bets = sb.table("bets").select(
            "implied_prob,edge_at_bet,result,market,stake,profit_loss"
        ).execute().data or []
        return live, bets

    live, all_bets   = load_alert_data()
    settled          = [b for b in all_bets if b.get("result") not in ("Pending", None, "Void")]
    bankroll         = float(st.session_state.get("bankroll", 500.0))
    calibration, mkt_roi, adaptive = _compute_learning_engine(settled)

    ALERT_MARKETS = [
        ("Win",    "win_prob",   "Win"),
        ("Top 5",  "top5_prob",  "Top 5"),
        ("Top 10", "top10_prob", "Top 10"),
    ]
    BOOKS = ["DraftKings", "FanDuel", "BetMGM", "Caesars", "Bet365", "theScore", "Hard Rock"]

    st.markdown('<div class="section-header">🚨 Live Model Alerts — Learning Engine</div>',
                unsafe_allow_html=True)

    # ─ Engine summary cards ───────────────────────────────────────────────────────
    live_round = any((p.get("thru") or 0) > 0 for p in live)

    if calibration:
        avg_err = sum(abs(v["diff"]) for v in calibration.values()) / len(calibration)
        if avg_err < 5:    calib_badge, calib_color = "🟢 Sharp",    "#69f0ae"
        elif avg_err < 10: calib_badge, calib_color = "🟡 Drifting", "#ffcc02"
        else:              calib_badge, calib_color = "🔴 Off",      "#ef9a9a"
    else:
        calib_badge, calib_color, avg_err = "⚪ No data yet", "#90a4ae", 0.0

    best_mkt     = max(mkt_roi, key=lambda m: mkt_roi[m]["roi"]) if mkt_roi else None
    best_mkt_roi = mkt_roi[best_mkt]["roi"] if best_mkt else 0.0

    t_win = adaptive.get("Win",    {}).get("threshold", _BASE_THRESHOLDS["Win"])
    t_t5  = adaptive.get("Top 5",  {}).get("threshold", _BASE_THRESHOLDS["Top 5"])
    t_t10 = adaptive.get("Top 10", {}).get("threshold", _BASE_THRESHOLDS["Top 10"])

    n_alerts = 0
    if live_round:
        for p in live:
            if not (p.get("thru") or 0):
                continue
            for _, prob_field, mkt_key in ALERT_MARKETS:
                prob = american_to_implied(p.get(prob_field)) or 0
                if prob >= adaptive.get(mkt_key, {}).get("threshold",
                                        _BASE_THRESHOLDS.get(mkt_key, 20)):
                    n_alerts += 1

    sm1, sm2, sm3 = st.columns(3)
    with sm1:
        st.markdown(f"""<div class="metric-card">
            <div class="label">Model Calibration</div>
            <div class="value" style="color:{calib_color}">{calib_badge}</div>
            <div class="sub">{len(calibration)} buckets tracked · avg {avg_err:.1f}pp error</div>
        </div>""", unsafe_allow_html=True)
    with sm2:
        roi_c = "#69f0ae" if best_mkt_roi > 0 else "#ef9a9a"
        st.markdown(f"""<div class="metric-card">
            <div class="label">Best Market ROI</div>
            <div class="value" style="color:{roi_c}">{best_mkt_roi:+.1f}%</div>
            <div class="sub">{best_mkt or "—"} · {len(settled)} settled bets</div>
        </div>""", unsafe_allow_html=True)
    with sm3:
        al_c = "#69f0ae" if n_alerts > 0 else "#90a4ae"
        st.markdown(f"""<div class="metric-card">
            <div class="label">Active Signals</div>
            <div class="value" style="color:{al_c}">{n_alerts}</div>
            <div class="sub">Win ≥{t_win:.0f}% · T5 ≥{t_t5:.0f}% · T10 ≥{t_t10:.0f}%</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ─ Adaptive threshold status row ─────────────────────────────────────────────
    st.markdown("**Adaptive Thresholds** — recalibrate weekly as your bet history builds")
    AT_COLORS = {"neutral": "#90a4ae", "good": "#69f0ae",
                 "loose": "#a5d6a7",   "watch": "#ffcc02", "tight": "#ef9a9a"}
    AT_LABELS = {"neutral": "⚪ Base",  "good": "🟢 Calibrated",
                 "loose": "🟢 Loosened","watch": "🟡 Watching","tight": "🔴 Tightened"}
    tc1, tc2, tc3 = st.columns(3)
    for col, mkt_key in zip([tc1, tc2, tc3], ["Win", "Top 5", "Top 10"]):
        ad     = adaptive.get(mkt_key, {})
        t      = ad.get("threshold", _BASE_THRESHOLDS.get(mkt_key, 20))
        delta  = ad.get("delta", 0)
        status = ad.get("status", "neutral")
        note   = ad.get("note", "—")
        color  = AT_COLORS[status]
        label  = AT_LABELS[status]
        d_str  = f" ({'+' if delta > 0 else ''}{delta:.0f}pp)" if delta != 0 else ""
        with col:
            st.markdown(f"""
            <div style="background:#1a1a1a; border:1px solid {color}; border-radius:6px;
                        padding:10px 14px; margin:4px 0;">
                <div style="font-size:0.72rem; color:#81c784; text-transform:uppercase;
                             letter-spacing:0.08em; margin-bottom:2px">{mkt_key}</div>
                <div style="font-size:1.5rem; font-weight:700; color:{color}">{t:.0f}%{d_str}</div>
                <div style="font-size:0.75rem; color:{color}">{label}</div>
                <div style="font-size:0.72rem; color:#90a4ae; margin-top:3px">{note}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ─ Live alert cards ───────────────────────────────────────────────────────────
    tourn_done = live_round and sum(
        1 for p in live if (p.get("thru") or 0) >= 18
    ) >= len(live) * 0.85

    if tourn_done:
        n_pending_h2h = 0
        try:
            _pend = get_supabase().table("bets").select("market,result") \
                        .eq("result", "Pending").execute().data or []
            n_pending_h2h = sum(1 for b in _pend if "H2H" in (b.get("market") or ""))
        except Exception:
            pass
        grade_note = (f" · **{n_pending_h2h} pending H2H bet{'s' if n_pending_h2h != 1 else ''}** to grade"
                      if n_pending_h2h else "")
        st.markdown(f"""<div style="background:#1a2e1a; border:1px solid #4caf50;
                border-radius:6px; padding:14px 18px; margin:8px 0;">
            <div style="font-size:1rem; font-weight:700; color:#69f0ae">
                🏁 Tournament Complete — Final Results Posted
            </div>
            <div style="color:#a5d6a7; font-size:0.88rem; margin-top:4px">
                Live alerts are paused. Grade your results in
                <b>📝 Tracker → 🏌️ Grade H2H</b>{grade_note}
            </div>
        </div>""", unsafe_allow_html=True)
    elif not live_round:
        st.markdown("""<div class="info-box">
            ⏳ Live signals appear once the round begins.<br>
            Calibration and market analytics below are always available.
        </div>""", unsafe_allow_html=True)
    else:
        alerts = []
        for p in live:
            if not (p.get("thru") or 0):
                continue
            pos = p.get("current_pos") or 999
            for mkt_label, prob_field, mkt_key in ALERT_MARKETS:
                prob      = american_to_implied(p.get(prob_field)) or 0
                threshold = adaptive.get(mkt_key, {}).get("threshold",
                            _BASE_THRESHOLDS.get(mkt_key, 20))
                if prob < threshold:
                    continue
                be_val = _breakeven_odds(prob)
                be_str = (f"+{be_val}" if be_val and be_val > 0 else str(be_val)) if be_val else "—"
                score  = _confidence_score(prob, mkt_key, calibration, mkt_roi)
                ks     = _kelly_stake(prob, be_val, bankroll)
                alerts.append({
                    "player":      p.get("player_name", ""),
                    "pos":         pos,
                    "market":      mkt_label,
                    "mkt_key":     mkt_key,
                    "prob":        prob,
                    "be_str":      be_str,
                    "be_val":      be_val,
                    "score":       score,
                    "kelly":       ks,
                    "calib_note":  _calib_note(prob, calibration),
                    "calib_ratio": _calib_factor(prob, calibration),
                })
        alerts.sort(key=lambda x: -x["score"])

        if not alerts:
            st.markdown(f"""<div class="info-box">
                No signals above adaptive thresholds right now
                (Win ≥{t_win:.0f}% · Top 5 ≥{t_t5:.0f}% · Top 10 ≥{t_t10:.0f}%).<br>
                Thresholds tighten/loosen automatically as your bet history builds.
            </div>""", unsafe_allow_html=True)
        else:
            st.markdown(f"**{len(alerts)} signal{'s' if len(alerts) != 1 else ''}** — "
                        "ranked by confidence score")
            st.caption("Confidence = DG probability × calibration accuracy × market ROI history. "
                       "Kelly stake scales to your bankroll (sidebar).")

            for i, a in enumerate(alerts):
                sc = a["score"]
                if sc >= 70:   border, tier = "#69f0ae", "HIGH CONFIDENCE"
                elif sc >= 50: border, tier = "#ffcc02", "MED CONFIDENCE"
                else:          border, tier = "#81c784", "LOW CONFIDENCE"

                cf     = a["calib_ratio"]
                cf_c   = "#69f0ae" if cf >= 1.05 else ("#ef9a9a" if cf < 0.90 else "#90a4ae")
                bar    = "█" * round(sc / 10) + "░" * (10 - round(sc / 10))
                ks_str = f"${a['kelly']:.0f}" if (a["kelly"] and a["kelly"] > 0) else "No edge"

                c_card, c_btn = st.columns([6, 1])
                with c_card:
                    st.markdown(f"""
                    <div style="border-left:4px solid {border}; padding:14px 18px; margin:8px 0;
                                background:#1a1a1a; border-radius:4px;">
                        <div style="display:flex; justify-content:space-between; align-items:baseline">
                            <span style="font-size:1.08rem; font-weight:700; color:{border}">
                                🎯 {a['player']} — {a['market']}
                                <span style="color:#777; font-weight:400; font-size:0.83rem;
                                            margin-left:8px">T{a['pos']}</span>
                            </span>
                            <span style="font-size:0.78rem; color:{border};
                                         font-weight:600">{tier}</span>
                        </div>
                        <div style="display:flex; gap:28px; flex-wrap:wrap; margin-top:8px">
                            <div>
                                <div style="color:#90a4ae; font-size:0.75rem">DG Model</div>
                                <div style="color:{border}; font-weight:700; font-size:1.1rem">{a['prob']:.1f}%</div>
                            </div>
                            <div>
                                <div style="color:#90a4ae; font-size:0.75rem">Confidence</div>
                                <div style="font-family:monospace; font-size:0.9rem; color:{border}">{bar} {sc}/100</div>
                            </div>
                            <div>
                                <div style="color:#90a4ae; font-size:0.75rem">Breakeven</div>
                                <div style="color:#fff; font-weight:700; font-size:1rem">{a['be_str']}</div>
                            </div>
                            <div>
                                <div style="color:#90a4ae; font-size:0.75rem">½-Kelly on ${bankroll:.0f}</div>
                                <div style="color:#ffcc02; font-weight:700; font-size:1rem">{ks_str}</div>
                            </div>
                        </div>
                        <div style="color:{cf_c}; font-size:0.76rem; margin-top:7px">
                            📊 {a['calib_note']}
                        </div>
                    </div>""", unsafe_allow_html=True)
                with c_btn:
                    st.markdown("<br>", unsafe_allow_html=True)
                    if st.button("📝 Log", key=f"al_open_{i}"):
                        st.session_state[f"al_form_{i}"] = not st.session_state.get(
                            f"al_form_{i}", False)

                if st.session_state.get(f"al_form_{i}"):
                    with st.container():
                        fc1, fc2, fc3, fc4 = st.columns([2, 2, 2, 1])
                        with fc1:
                            q_book = st.selectbox("Book", BOOKS, key=f"al_bk_{i}")
                        with fc2:
                            q_odds = st.number_input("Odds (American)", value=0, step=5,
                                                     key=f"al_od_{i}",
                                                     help="e.g. -150 or +320")
                        with fc3:
                            ks_def  = (a["kelly"] if (a["kelly"] and a["kelly"] > 0)
                                       else float(st.session_state.get("default_stake", 10)))
                            q_stake = st.number_input("Stake $", value=float(ks_def),
                                                      step=5.0, key=f"al_st_{i}")
                        with fc4:
                            st.markdown("<br>", unsafe_allow_html=True)
                            log_btn = st.button("✅ Log", key=f"al_log_{i}")

                        if q_odds != 0:
                            q_implied = american_to_implied(q_odds) or 0
                            q_edge    = round(a["prob"] - q_implied, 2)
                            q_kelly   = _kelly_stake(a["prob"], q_odds, bankroll)
                            e_color   = "#69f0ae" if q_edge > 0 else "#ef9a9a"
                            verdict   = "✅ VALUE" if q_edge > 0 else "❌ NO EDGE"
                            ks_note   = (f" · ½-Kelly: ${q_kelly:.0f}"
                                         if (q_kelly and q_kelly > 0) else "")
                            st.markdown(
                                f"<span style='color:{e_color}; font-weight:700'>{verdict}</span>"
                                f" &nbsp; Edge: <b style='color:{e_color}'>{q_edge:+.2f}%</b>"
                                f" &nbsp;·&nbsp; DG: {a['prob']:.1f}% vs Book: {q_implied:.1f}%"
                                f"<span style='color:#ffcc02'>{ks_note}</span>",
                                unsafe_allow_html=True,
                            )

                        if log_btn:
                            if q_odds != 0:
                                q_implied = american_to_implied(q_odds) or 0
                                ok = quick_log_bet(
                                    player=a["player"], market=a["market"],
                                    book=q_book, odds=q_odds,
                                    edge=round(a["prob"] - q_implied, 2),
                                    stake=q_stake, event=current_event,
                                    notes=(f"Live alert · DG {a['prob']:.1f}% {a['market']} · "
                                           f"Score {a['score']}/100 · Calib×{a['calib_ratio']:.2f}"),
                                )
                                if ok:
                                    st.success(f"✅ Logged: {a['player']} {a['market']} "
                                               f"@ {'+' if q_odds > 0 else ''}{q_odds}")
                                    st.session_state[f"al_form_{i}"] = False
                                    st.cache_data.clear()
                                else:
                                    st.error("Failed to log bet")
                            else:
                                st.warning("Enter the odds from your book first")

    st.markdown("---")

    # ─ Model Calibration ─────────────────────────────────────────────────────────
    with st.expander("📊 Model Calibration — Predicted vs. Actual Hit Rate", expanded=False):
        if len(settled) < 3:
            st.info("Need at least 3 settled bets to compute calibration. "
                    "Bets logged from this tab automatically record DG probability.")
        elif not calibration:
            st.info("Calibration data populates as bets settle. "
                    "Make sure bets have edge_at_bet and implied_prob recorded.")
        else:
            st.caption(
                "Compares DG model's predicted probability to the actual % of logged bets "
                "that won. A well-calibrated model has Actual ≈ Predicted. "
                "Ratio >1.0x means the model underestimates players at that range — "
                "alerts in that bucket get a confidence boost. Ratio <1.0x means scale back."
            )
            calib_rows = []
            for lb in _LE_BUCKET_ORDER:
                if lb not in calibration:
                    continue
                c    = calibration[lb]
                diff = c["diff"]
                if abs(diff) < 3:   stat = "✅ On target"
                elif diff > 0:      stat = f"🔺 Underestimates (+{diff:.0f}pp)"
                else:               stat = f"🔻 Overestimates ({diff:.0f}pp)"
                calib_rows.append({
                    "Prob Range":   lb,
                    "DG Predicted": f"{c['predicted']:.1f}%",
                    "Actual Hit":   f"{c['actual']:.1f}%",
                    "Δ":            f"{diff:+.1f}pp",
                    "Ratio (A/P)":  f"{c['ratio']:.2f}x",
                    "Bets":         c["n"],
                    "Status":       stat,
                })

            def _col_calib(val):
                if "target"       in str(val): return "color:#69f0ae"
                if "Underestimates" in str(val): return "color:#a5d6a7"
                if "Overestimates"  in str(val): return "color:#ef9a9a"
                return ""

            st.dataframe(
                pd.DataFrame(calib_rows).style.map(_col_calib, subset=["Status"]),
                use_container_width=True, hide_index=True,
            )

    # ─ Market Health ──────────────────────────────────────────────────────────────
    with st.expander("📈 Market Health — ROI & Adaptive Thresholds", expanded=False):
        if len(settled) < 3:
            st.info("Need at least 3 settled bets to compute market health.")
        else:
            st.caption(
                "Adaptive thresholds tighten (+pp) when ROI < 0 and loosen (−pp) when "
                "ROI > 10%. Requires 5+ settled bets per market to activate."
            )
            AT_STAT_LABELS = {
                "good": "✅ Calibrated", "loose": "🟢 Loosened",
                "watch": "🟡 Watching",  "tight": "🔴 Tightened", "neutral": "⚪ Base",
            }
            mkt_rows = []
            for mkt, md in sorted(mkt_roi.items(), key=lambda x: -x[1]["roi"]):
                ad    = adaptive.get(mkt, {})
                base  = _BASE_THRESHOLDS.get(mkt)
                curr  = ad.get("threshold", base)
                delta = ad.get("delta", 0)
                stat  = AT_STAT_LABELS.get(ad.get("status", "neutral"), "⚪ Base")
                mkt_rows.append({
                    "Market":      mkt,
                    "Settled":     md["n"],
                    "Win Rate":    f"{md['win_rate']:.1f}%",
                    "P&L":         f"${md['pl']:+.2f}",
                    "ROI":         md["roi"],
                    "Base Thresh": f"{base:.0f}%" if base else "—",
                    "Adaptive":    f"{curr:.0f}%" if curr else "—",
                    "Δ":           (f"{'+' if delta > 0 else ''}{delta:.0f}pp"
                                    if delta else "—"),
                    "Status":      stat,
                })

            def _col_roi(val):
                if isinstance(val, (int, float)):
                    if val > 5:  return "color:#69f0ae; font-weight:600"
                    if val > 0:  return "color:#a5d6a7"
                    return "color:#ef9a9a"
                return ""

            def _col_mstat(val):
                if "Calibrated" in str(val): return "color:#69f0ae"
                if "Loosened"   in str(val): return "color:#a5d6a7"
                if "Watching"   in str(val): return "color:#ffcc02"
                if "Tightened"  in str(val): return "color:#ef9a9a"
                return "color:#90a4ae"

            st.dataframe(
                pd.DataFrame(mkt_rows).style
                    .map(_col_roi,   subset=["ROI"])
                    .map(_col_mstat, subset=["Status"])
                    .format({"ROI": "{:+.1f}%"}),
                use_container_width=True, hide_index=True,
            )

            # ─ Snapshot history ──────────────────────────────────────────────
            st.markdown("---")
            sh1, sh2 = st.columns([5, 1])
            with sh1:
                st.markdown("**Performance History** — weekly calibration & ROI over time")
            with sh2:
                if st.button("📸 Save Snapshot", key="save_snap"):
                    try:
                        sb2 = get_supabase()
                        sb2.table("model_snapshots").insert({
                            "snapshot_at":   (datetime.now(timezone.utc)
                                              - timedelta(hours=4)).isoformat(),
                            "event_name":    current_event,
                            "total_settled": len(settled),
                            "calibration":   calibration,
                            "market_roi":    mkt_roi,
                            "adaptive_thresholds": {
                                k: {"threshold": v.get("threshold"),
                                    "delta":     v.get("delta", 0),
                                    "status":    v.get("status", "neutral")}
                                for k, v in adaptive.items()
                            },
                        }).execute()
                        st.success("Snapshot saved!")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"Failed: {e}")
                        st.code(
                            "CREATE TABLE IF NOT EXISTS model_snapshots (\n"
                            "    id bigserial primary key,\n"
                            "    snapshot_at timestamptz default now(),\n"
                            "    event_name text,\n"
                            "    total_settled integer,\n"
                            "    calibration jsonb,\n"
                            "    market_roi jsonb,\n"
                            "    adaptive_thresholds jsonb\n"
                            ");",
                            language="sql",
                        )

            @st.cache_data(ttl=300)
            def load_snapshots():
                return (get_supabase()
                        .table("model_snapshots")
                        .select("snapshot_at,event_name,total_settled,market_roi,adaptive_thresholds")
                        .order("snapshot_at", desc=True)
                        .limit(12)
                        .execute().data or [])

            snaps = load_snapshots()
            if snaps:
                snap_rows = []
                for s in snaps:
                    ts   = (s.get("snapshot_at") or "")[:16].replace("T", " ")
                    mro  = s.get("market_roi") or {}
                    ath  = s.get("adaptive_thresholds") or {}
                    snap_rows.append({
                        "Saved":       ts,
                        "Event":       s.get("event_name", "—"),
                        "Settled":     s.get("total_settled", 0),
                        "Win ROI":     mro.get("Win",    {}).get("roi"),
                        "Top 5 ROI":   mro.get("Top 5",  {}).get("roi"),
                        "Top 10 ROI":  mro.get("Top 10", {}).get("roi"),
                        "Win Thresh":  (f"{ath.get('Win',{}).get('threshold',20):.0f}%"
                                        if ath.get("Win") else "20%"),
                    })

                def _col_sroi(val):
                    if isinstance(val, (int, float)):
                        if val > 5:  return "color:#69f0ae; font-weight:600"
                        if val > 0:  return "color:#a5d6a7"
                        return "color:#ef9a9a"
                    return ""

                st.dataframe(
                    pd.DataFrame(snap_rows).style
                        .map(_col_sroi, subset=["Win ROI", "Top 5 ROI", "Top 10 ROI"])
                        .format({"Win ROI":    "{:+.1f}%",
                                 "Top 5 ROI":  "{:+.1f}%",
                                 "Top 10 ROI": "{:+.1f}%"}, na_rep="—"),
                    use_container_width=True, hide_index=True,
                )
                st.caption("Also auto-saved by golf_sync.py --mode full after each tournament.")
            else:
                st.caption("No snapshots yet — click Save Snapshot or run a full sync "
                           "after the tournament settles.")


# ── Tab routing ──────────────────────────────────────────────────────────────────
with tab_forecast:
    _fv = st.radio("", ["📊 Tournament Forecast", "📈 Skill Ratings"],
                   horizontal=True, label_visibility="collapsed", key="tab_fv")
    if _fv == "📊 Tournament Forecast":
        _render_tournament_forecast()
    else:
        _render_skill_ratings()

with tab_edges:
    _ev = st.radio("", ["💰 Finish Odds + Edge", "📚 Best Plays by Book"],
                   horizontal=True, label_visibility="collapsed", key="tab_ev")
    if _ev == "💰 Finish Odds + Edge":
        _render_finish_odds()
    else:
        _render_best_plays_by_book()

with tab_h2h:
    _hv = st.radio("", ["🎯 Best H2H Plays", "⚔️ Matchup Tool"],
                   horizontal=True, label_visibility="collapsed", key="tab_hv")
    if _hv == "🎯 Best H2H Plays":
        _render_best_h2h()
    else:
        _render_matchup_tool()

with tab_live:
    _lv = st.radio("", ["🏆 Live Leaderboard", "🔴 Live Matchups"],
                   horizontal=True, label_visibility="collapsed", key="tab_lv")
    if _lv == "🏆 Live Leaderboard":
        _render_leaderboard()
    else:
        _render_live_matchups()

with tab_alerts:
    _render_live_alerts()

with tab_tracker:
    _tv = st.radio("", ["📝 Results Tracker", "⚙️ Auto Scheduler"],
                   horizontal=True, label_visibility="collapsed", key="tab_tv")
    if _tv == "📝 Results Tracker":
        _render_tracker()
    else:
        _render_auto_scheduler()

with tab_research:
    _render_course_history()

# ── Footer ───────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("""
<div style="text-align:center; color:#4caf50; font-size:0.75rem; padding:0.5rem">
    ⛳ Golf Betting Model · DataGolf + The Odds API · Built by Jet Excellence Analytics
</div>
""", unsafe_allow_html=True)

