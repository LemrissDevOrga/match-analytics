"""
scripts/seed_supabase.py

One-time script to load all existing CSV data into Supabase.
All column names are lowercased to match PostgreSQL schema.

Usage:
    pip install supabase python-dotenv
    python scripts/seed_supabase.py
"""

import os
import csv
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

LEAGUES = [
    "premier_league", "laliga", "champions_league",
    "bundesliga", "serie_a", "ligue1", "eredivisie", "primeira_liga"
]

SOFA_SEASONS  = ["21-22", "22-23", "23-24", "24-25", "25-26"]
BASIC_SEASONS = ["2023", "2024", "2025"]
SEASON_MAP    = {"2023": "23-24", "2024": "24-25", "2025": "25-26"}

NUMERIC_FIELDS = {
    # matches
    "home_goals","away_goals",
    "home_ballpossession","away_ballpossession",
    "home_expectedgoals","away_expectedgoals",
    "home_bigchancecreated","away_bigchancecreated",
    "home_bigchancemissed","away_bigchancemissed",
    "home_totalshotsongoal","away_totalshotsongoal",
    "home_shotsontarget","away_shotsontarget",
    "home_shotsofftarget","away_shotsofftarget",
    "home_blockedshots","away_blockedshots",
    "home_goalkeepersaves","away_goalkeepersaves",
    "home_cornerkicks","away_cornerkicks",
    "home_yellowcards","away_yellowcards",
    "home_redcards","away_redcards",
    "home_accuratepasses","away_accuratepasses",
    "home_accuratepassespercentage","away_accuratepassespercentage",
    "home_fouls","away_fouls",
    "home_offsides","away_offsides",
    "home_totalduels","away_totalduels",
    "home_totalduelswon","away_totalduelswon",
    # team_stats
    "played","wins","draws","losses","points",
    "goals_scored","goals_conceded","goal_difference","clean_sheets",
    "avg_goals_scored","home_played","home_wins","away_played","away_wins",
    "home_win_rate","away_win_rate",
    # advanced
    "avg_ballpossession","home_avg_ballpossession","away_avg_ballpossession",
    "avg_expectedgoals","home_avg_expectedgoals","away_avg_expectedgoals",
    "avg_bigchancecreated","home_avg_bigchancecreated","away_avg_bigchancecreated",
    "avg_bigchancemissed","home_avg_bigchancemissed","away_avg_bigchancemissed",
    "avg_totalshotsongoal","home_avg_totalshotsongoal","away_avg_totalshotsongoal",
    "avg_goalkeepersaves","home_avg_goalkeepersaves","away_avg_goalkeepersaves",
    "avg_cornerkicks","home_avg_cornerkicks","away_avg_cornerkicks",
    "avg_yellowcards","home_avg_yellowcards","away_avg_yellowcards",
    "avg_redcards","home_avg_redcards","away_avg_redcards",
    "avg_accuratepasses","home_avg_accuratepasses","away_avg_accuratepasses",
    "avg_fouls","home_avg_fouls","away_avg_fouls",
    "avg_offsides","home_avg_offsides","away_avg_offsides",
}

MATCHES_COLS = {
    "league","season","date","home_team","away_team","home_goals","away_goals",
    "stage","duration",
    "home_ballpossession","away_ballpossession","home_expectedgoals","away_expectedgoals",
    "home_bigchancecreated","away_bigchancecreated","home_bigchancemissed","away_bigchancemissed",
    "home_totalshotsongoal","away_totalshotsongoal","home_shotsontarget","away_shotsontarget",
    "home_shotsofftarget","away_shotsofftarget","home_blockedshots","away_blockedshots",
    "home_goalkeepersaves","away_goalkeepersaves","home_cornerkicks","away_cornerkicks",
    "home_yellowcards","away_yellowcards","home_redcards","away_redcards",
    "home_accuratepasses","away_accuratepasses","home_accuratepassespercentage","away_accuratepassespercentage",
    "home_fouls","away_fouls","home_offsides","away_offsides",
    "home_totalduels","away_totalduels","home_totalduelswon","away_totalduelswon",
}

TEAM_STATS_COLS = {
    "league","season","team","played","wins","draws","losses","points",
    "goals_scored","goals_conceded","goal_difference","clean_sheets","form_last5",
    "avg_goals_scored","home_played","home_wins","away_played","away_wins",
    "home_win_rate","away_win_rate",
}

ADVANCED_COLS = {
    "league","season","team",
    "avg_ballpossession","home_avg_ballpossession","away_avg_ballpossession",
    "avg_expectedgoals","home_avg_expectedgoals","away_avg_expectedgoals",
    "avg_bigchancecreated","home_avg_bigchancecreated","away_avg_bigchancecreated",
    "avg_bigchancemissed","home_avg_bigchancemissed","away_avg_bigchancemissed",
    "avg_totalshotsongoal","home_avg_totalshotsongoal","away_avg_totalshotsongoal",
    "avg_goalkeepersaves","home_avg_goalkeepersaves","away_avg_goalkeepersaves",
    "avg_cornerkicks","home_avg_cornerkicks","away_avg_cornerkicks",
    "avg_yellowcards","home_avg_yellowcards","away_avg_yellowcards",
    "avg_redcards","home_avg_redcards","away_avg_redcards",
    "avg_accuratepasses","home_avg_accuratepasses","away_avg_accuratepasses",
    "avg_fouls","home_avg_fouls","away_avg_fouls",
    "avg_offsides","home_avg_offsides","away_avg_offsides",
}


def read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def clean_row(row, allowed_cols):
    """Lowercase all keys, filter to allowed cols, cast numerics."""
    out = {}
    for k, v in row.items():
        lk = k.lower()
        if lk not in allowed_cols:
            continue
        if v == "" or v is None:
            out[lk] = None
        elif lk in NUMERIC_FIELDS:
            try:
                out[lk] = float(v)
            except (ValueError, TypeError):
                out[lk] = None
        else:
            out[lk] = v
    return out


def upsert_batch(table, rows, conflict_key, batch_size=500):
    total = len(rows)
    for i in range(0, total, batch_size):
        batch = rows[i:i+batch_size]
        supabase.table(table).upsert(batch, on_conflict=conflict_key).execute()
        print(f"    ✅ {min(i+batch_size, total)}/{total} rows")


def seed_matches():
    print("\n📥 Seeding matches…")
    for league in LEAGUES:
        for season in SOFA_SEASONS:
            path = f"data/processed_sofascore/{league}/{league}_{season}.csv"
            rows = read_csv(path)
            if not rows:
                continue
            cleaned = []
            for r in rows:
                row = clean_row(r, MATCHES_COLS)
                row["league"] = league
                row["season"] = season
                cleaned.append(row)
            print(f"  🏆 {league} {season} — {len(cleaned)} matches")
            upsert_batch("matches", cleaned, "league,season,date,home_team,away_team")


def seed_team_stats():
    print("\n📥 Seeding team_stats…")
    for league in LEAGUES:
        for season in BASIC_SEASONS:
            path = f"data/analytics/{league}/{league}_{season}_team_stats.csv"
            rows = read_csv(path)
            if not rows:
                continue
            season_label = SEASON_MAP[season]
            cleaned = []
            for r in rows:
                row = clean_row(r, TEAM_STATS_COLS)
                row["league"] = league
                row["season"] = season_label
                cleaned.append(row)
            print(f"  🏆 {league} {season_label} — {len(cleaned)} teams")
            upsert_batch("team_stats", cleaned, "league,season,team")


def seed_advanced_stats():
    print("\n📥 Seeding advanced_stats…")
    for league in LEAGUES:
        for season in SOFA_SEASONS:
            path = f"data/analytics/{league}/{league}_{season}_advanced.csv"
            rows = read_csv(path)
            if not rows:
                continue
            cleaned = []
            for r in rows:
                row = clean_row(r, ADVANCED_COLS)
                row["league"] = league
                row["season"] = season
                cleaned.append(row)
            print(f"  🏆 {league} {season} — {len(cleaned)} teams")
            upsert_batch("advanced_stats", cleaned, "league,season,team")


if __name__ == "__main__":
    print("🚀 Seeding Supabase from local CSVs…")
    seed_matches()
    seed_team_stats()
    seed_advanced_stats()
    print("\n✅ All done!")