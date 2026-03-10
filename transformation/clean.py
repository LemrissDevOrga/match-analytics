"""
transformation/clean.py

Silver layer: reads football-data.org raw JSON → upserts into `matches` table.
Replaces the old CSV output in data/processed/.
"""

import json
import os
import sys

# Allow importing from scripts/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from scripts.db import get_conn, upsert

LEAGUES = [
    "laliga", "premier_league", "champions_league",
    "bundesliga", "serie_a", "ligue1", "eredivisie", "primeira_liga",
]


def parse_match(m, league):
    """Parse a single football-data.org match object into a flat dict."""
    score = m.get("score", {})
    ft = score.get("fullTime", {})
    ht = score.get("halfTime", {})
    home = m.get("homeTeam", {})
    away = m.get("awayTeam", {})
    season_obj = m.get("season", {})

    # Derive season year key from startDate e.g. "2024-08-..." → "2024"
    # We use the START year to match LEAGUE_CONFIG seasonLabels keys
    start = season_obj.get("startDate", "")
    season = start[:4] if start else ""

    return {
        "match_id":         str(m.get("id", "")),
        "league":           league,
        "season":           season,
        "matchday":         m.get("matchday"),
        "date":             m.get("utcDate", "")[:10] or None,
        "status":           m.get("status"),
        "stage":            m.get("stage"),
        "home_team":        home.get("name"),
        "home_team_short":  home.get("shortName"),
        "home_team_tla":    home.get("tla"),
        "away_team":        away.get("name"),
        "away_team_short":  away.get("shortName"),
        "away_team_tla":    away.get("tla"),
        "home_goals_ft":    ft.get("home"),
        "away_goals_ft":    ft.get("away"),
        "home_goals_ht":    ht.get("home"),
        "away_goals_ht":    ht.get("away"),
        "result":           score.get("winner"),
        "duration":         score.get("duration"),
        "referee":          (m.get("referees") or [{}])[0].get("name"),
    }


def load_raw(league):
    """Yield all match objects from raw JSON files for a league."""
    raw_dir = f"data/raw/{league}"
    if not os.path.exists(raw_dir):
        return
    for fname in sorted(os.listdir(raw_dir)):
        if not fname.endswith(".json"):
            continue
        with open(f"{raw_dir}/{fname}", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                print(f"  ⚠️  JSON error in {fname}, skipping")
                continue
        # football-data returns {"matches": [...]} or a list directly
        if isinstance(data, list):
            yield from data
        elif isinstance(data, dict):
            yield from data.get("matches", [])


def main():
    print("🔄 clean.py — football-data.org JSON → Supabase matches\n")
    conn = get_conn()

    for league in LEAGUES:
        print(f"🏆 {league.replace('_', ' ').title()}")
        rows = []
        for m in load_raw(league):
            try:
                rows.append(parse_match(m, league))
            except Exception as e:
                print(f"  ⚠️  Parse error: {e}")

        if rows:
            upsert(conn, "matches", rows, conflict_cols=["match_id", "league"])
        else:
            print(f"  ℹ️  No data found")

    conn.close()
    print("\n✅ Done")


if __name__ == "__main__":
    main()