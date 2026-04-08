"""
golf_app.py — Golf H2H Betting Model Dashboard
Deploy to Streamlit Cloud via GitHub (same repo as MLB F5 model)
Run locally: streamlit run golf_app.py
"""

import streamlit as st
import pandas as pd
from supabase import create_client
from datetime import datetime

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Golf Betting Model",
    page_icon="⛳",
    layout="wide",
    initial_sidebar_state="collapsed",
)

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
    skill    = sb.table("skill_ratings").select("*").order("dg_rank").execute().data
    field    = sb.table("field").select("*").execute().data
    preds    = sb.table("predictions").select("*").execute().data
    fin_odds = sb.table("finish_odds").select("*").execute().data
    matchups = sb.table("matchup_odds").select("*").order("p1_dg_win_prob", desc=True).execute().data
    rounds   = sb.table("rounds").select("*").order("year", desc=True).limit(25000).execute().data
    schedule = sb.table("schedule").select("*").order("start_date", desc=True).execute().data
    return skill, field, preds, fin_odds, matchups, rounds, schedule

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

SHARP_THRESHOLDS = {
    "win":      2.0,
    "top_5":    3.0,
    "top_10":   3.5,
    "top_20":   4.0,
    "make_cut": 5.0,
    "matchup":  2.0,
}

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
        skill, field, preds, fin_odds, matchups, rounds, schedule = load_data()
    except Exception as e:
        st.error(f"Could not connect to database: {e}")
        st.stop()

# Index data
skill_by_id  = {int(p["dg_id"]): p for p in skill if p.get("dg_id")}
field_ids    = {int(p["dg_id"]) for p in field if p.get("dg_id")}
pred_by_id   = {int(p["dg_id"]): p for p in preds if p.get("dg_id")}
fo_index     = {(int(fo["dg_id"]), fo["market"]): fo for fo in fin_odds if fo.get("dg_id")}

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
    for key in ["draftkings", "fanduel", "betmgm", "caesars", "bet365", "best_odds"]:
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
        "bl_win":   w_prob,   "bl_top5":  t5_prob,
        "bl_top10": t10_prob, "bl_cut":   c_prob,
        "co_win":   cw_prob,
        # _p keys used by finish odds tab (prob_key = f"{mk}_p")
        "w_p":      w_prob,
        "t5_p":     t5_prob,
        "t10_p":    t10_prob,
        "t20_p":    t20_prob,
        "c_p":      c_prob,
        # win odds
        "w_dg_p": w_prob,    "w_dg":  fo_w.get("dg_odds"),
        "w_dk":   fo_w.get("draftkings"), "w_fd":  fo_w.get("fanduel"),
        "w_mgm":  fo_w.get("betmgm"),     "w_czr": fo_w.get("caesars"),
        "w_365":  fo_w.get("bet365"),     "w_best":w_best_odds,
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
            ("p1", "p1_dg_odds", ["p1_best_odds","p1_bet365","p1_bovada"]),
            ("p2", "p2_dg_odds", ["p2_best_odds","p2_bet365","p2_bovada"]),
        ]
        for bk_odds in [[m.get(k) for k in bk_keys if m.get(k)]]
        if bk_odds and edge_pct(
            american_to_implied(m.get(dg_key)),
            bk_odds[0]
        ) and (edge_pct(american_to_implied(m.get(dg_key)), bk_odds[0]) or 0) >= SHARP_THRESHOLDS["matchup"]
    ) if matchups else 0
    st.markdown(f"""<div class="metric-card">
        <div class="label">Last Synced</div>
        <div class="value" style="font-size:1rem">{datetime.now().strftime("%I:%M %p")}</div>
        <div class="sub">{datetime.now().strftime("%b %d, %Y")} · {h2h_sharp} H2H sharp plays</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Tabs ─────────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📊 Tournament Forecast",
    "💰 Finish Odds + Edge",
    "⚔️ H2H Matchup Tool",
    "🎯 Best H2H Plays",
    "📈 Skill Ratings",
    "🏌️ Course History",
    "🔴 Live Matchups",
])

# ════════════════════════════════════════════════════════════
# TAB 1 — TOURNAMENT FORECAST
# ════════════════════════════════════════════════════════════
with tab1:
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
        if e_w < min_edge:
            continue
        ew,  sharp_w  = sharp_value(p["w_dg_p"], p["w_best"],  "win")
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
# TAB 2 — FINISH ODDS + EDGE
# ════════════════════════════════════════════════════════════
with tab2:
    st.markdown('<div class="section-header">Finish Position Odds — Best Available Across All Books</div>', unsafe_allow_html=True)

    market_sel = st.radio("Market", ["Win", "Top 5", "Top 10", "Top 20", "Make Cut"],
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

    rows2 = []
    for p in field_players:
        prob  = p.get(prob_key)
        best  = p.get(best_key)
        e     = edge_pct(prob, best)
        sv_edge, sv = sharp_value(prob, best, mk_threshold)
        sv_display = f"{sv} +{sv_edge:.2f}%" if sv and sv_edge else (sv or "—")
        row = {
            "Player":      p["name"],
            "DG Rank":     p["dg_rank"] or "NR",
            "DG Prob%":    round((prob or 0), 2),
            "Best Odds":   fmt_odds(best),
            "Best Book":   (p.get(bk_key) or "").title(),
            "Edge%":       round(e, 2) if e is not None else 0.0,
            "Sharp Value": sv_display,
        }
        if dk_key:
            row["DraftKings"] = fmt_odds(p.get(dk_key))
        if fd_key:
            row["FanDuel"] = fmt_odds(p.get(fd_key))
        if mk == "w":
            row["BetMGM"]  = fmt_odds(p.get("w_mgm"))
            row["Caesars"] = fmt_odds(p.get("w_czr"))
            row["Bet365"]  = fmt_odds(p.get("w_365"))
        rows2.append(row)

    df2 = pd.DataFrame(rows2).sort_values("Edge%", ascending=False)

    min_e2 = st.slider("Min Edge%", -15.0, 15.0, -15.0, 0.5, label_visibility="collapsed")
    df2 = df2[df2["Edge%"] >= min_e2]

    def color_edge2(val):
        if isinstance(val, (int, float)):
            if val > 3:  return "background-color:#1b3a1b; color:#69f0ae; font-weight:700"
            if val > 0:  return "background-color:#1e3320; color:#a5d6a7"
            if val < -3: return "background-color:#3a1b1b; color:#ef9a9a"
            if val < 0:  return "background-color:#2d1e1e; color:#ef9a9a"
        return ""

    def color_sharp2(val):
        if isinstance(val, str):
            if "STRONG" in val: return "background-color:#1a3a1a; color:#69f0ae; font-weight:700"
            if "SHARP"  in val: return "background-color:#1e3320; color:#a5d6a7; font-weight:600"
            if "VALUE"  in val: return "background-color:#1b2e1b; color:#81c784"
        return ""

    fmt_cols = {"DG Prob%": "{:.2f}%", "Edge%": "{:+.2f}%"}
    styled2 = df2.style\
        .map(color_edge2, subset=["Edge%"])\
        .map(color_sharp2, subset=["Sharp Value"])\
        .format(fmt_cols, na_rep="—")
    st.dataframe(styled2, use_container_width=True, hide_index=True, height=500)

    pos = (df2["Edge%"] > 2).sum()
    st.markdown(f"""<div class="info-box">
        Market: <b>{market_sel}</b> · {len(df2)} players shown · 
        <span style="color:#69f0ae">{pos} positive edges &gt;2%</span>
    </div>""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# TAB 3 — H2H MATCHUP TOOL
# ════════════════════════════════════════════════════════════
with tab3:
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
# TAB 4 — BEST H2H PLAYS
# ════════════════════════════════════════════════════════════
with tab4:
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
        round_filter = st.selectbox("Round", ["All Rounds", "Round 1", "Round 2",
                                               "Round 3", "Round 4"],
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
            # Best available book line (stored in sync, fall back to individual books)
            p1_best = m.get("p1_best_odds") or m.get("p1_bet365") or m.get("p1_bovada") or m.get("p1_draftkings") or m.get("p1_fanduel")
            p2_best = m.get("p2_best_odds") or m.get("p2_bet365") or m.get("p2_bovada") or m.get("p2_draftkings") or m.get("p2_fanduel")
            p1_best_bk = m.get("p1_best_book", "")
            p2_best_bk = m.get("p2_best_book", "")
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
            df_h2h = pd.DataFrame(h2h_rows).sort_values("Edge%", ascending=False)

            def color_h2h_edge(val):
                if isinstance(val, (int, float)):
                    if val >= 5:  return "background-color:#1a3a1a; color:#69f0ae; font-weight:700"
                    if val >= 3:  return "background-color:#1e3320; color:#a5d6a7; font-weight:600"
                    if val >= 2:  return "background-color:#1b2e1b; color:#81c784"
                return ""

            def color_sharp_h2h(val):
                if isinstance(val, str):
                    if "STRONG" in val: return "background-color:#1a3a1a; color:#69f0ae; font-weight:700"
                    if "SHARP"  in val: return "background-color:#1e3320; color:#a5d6a7; font-weight:600"
                    if "VALUE"  in val: return "background-color:#1b2e1b; color:#81c784"
                return ""

            styled_h2h = df_h2h.style\
                .map(color_h2h_edge, subset=["Edge%"])\
                .map(color_sharp_h2h, subset=["Sharp Value"])\
                .format({
                    "DG Win%":    "{:.2f}%",
                    "Book Impl%": "{:.2f}%",
                    "Edge%":      "{:+.2f}%",
                }, na_rep="—")

            st.dataframe(styled_h2h, use_container_width=True, hide_index=True, height=520)
            st.markdown(f"""<div class="info-box">
                {len(df_h2h)} plays above {min_edge_h2h:.1f}% edge threshold ·
                Sorted by edge % ·
                🔥🔥 STRONG = ≥5% · 🔥 SHARP = ≥3% · ✅ VALUE = ≥2%
            </div>""", unsafe_allow_html=True)
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
# TAB 5 — SKILL RATINGS
# ════════════════════════════════════════════════════════════
with tab5:
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
# TAB 5 — COURSE HISTORY
# ════════════════════════════════════════════════════════════
with tab6:
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
# TAB 6 — LIVE MATCHUPS
# ════════════════════════════════════════════════════════════
with tab7:
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

# ── Footer ───────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("""
<div style="text-align:center; color:#4caf50; font-size:0.75rem; padding:0.5rem">
    ⛳ Golf Betting Model · DataGolf + The Odds API · Built by Jet Excellence Analytics
</div>
""", unsafe_allow_html=True)
