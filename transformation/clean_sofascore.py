"""
transformation/clean_sofascore.py

Silver layer: reads SofaScore raw JSON → upserts into `sofascore_matches` table.
Replaces the old CSV output in data/processed_sofascore/.
"""

import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from scripts.db import get_conn, upsert

LEAGUES = [
    "laliga", "premier_league", "champions_league",
    "bundesliga", "serie_a", "ligue1", "eredivisie", "primeira_liga",
]

STAT_KEYS = [
    "ballPossession", "expectedGoals", "bigChanceCreated", "bigChanceMissed",
    "totalShotsOnGoal", "shotsOnTarget", "shotsOffTarget", "blockedShots",
    "goalkeeperSaves", "cornerKicks", "yellowCards", "redCards",
    "totalPasses", "accuratePasses", "accuratePassesPercentage",
    "fouls", "offsides", "dribbles", "successfulDribbles",
    "tackles", "totalDuels", "totalDuelsWon",
]


def safe_float(val):
    try:
        return float(val) if val not in (None, "", "None") else None
    except (ValueError, TypeError):
        return None


def parse_event(event, league, season):
    """Parse a single SofaScore event JSON into a flat dict."""
    ts = event.get("startTimestamp", 0)
    date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d") if ts else None

    home = event.get("homeTeam", {})
    away = event.get("awayTeam", {})
    home_score = event.get("homeScore", {})
    away_score = event.get("awayScore", {})

    hg = safe_float(home_score.get("current"))
    ag = safe_float(away_score.get("current"))
    if hg is not None and ag is not None:
        result = "HOME_TEAM" if hg > ag else ("AWAY_TEAM" if ag > hg else "DRAW")
    else:
        result = None

    row = {
        "match_id":   str(event.get("id", "")),
        "league":     league,
        "season":     season,
        "date":       date,
        "home_team":  home.get("name"),
        "away_team":  away.get("name"),
        "home_goals": hg,
        "away_goals": ag,
        "result":     result,
    }

    # Flatten _stats — use snake_case column names
    import re as _re
    stats = event.get("_stats", {})
    for key in STAT_KEYS:
        snake = _re.sub(r'([A-Z])', r'_\1', key).lower()
        row[f"home_{snake}"] = safe_float(stats.get(f"home_{key}"))
        row[f"away_{snake}"] = safe_float(stats.get(f"away_{key}"))

    return row


def load_raw_sofascore(league):
    """Yield (event_dict, season_str) from all raw SofaScore JSON files."""
    raw_dir = f"data/raw_sofascore/{league}"
    if not os.path.exists(raw_dir):
        return
    for season_dir in sorted(os.listdir(raw_dir)):
        season_path = f"{raw_dir}/{season_dir}"
        if not os.path.isdir(season_path):
            continue
        season = season_dir  # e.g. "25-26"
        for fname in sorted(os.listdir(season_path)):
            if not fname.endswith(".json"):
                continue
            with open(f"{season_path}/{fname}", encoding="utf-8") as f:
                try:
                    event = json.load(f)
                except json.JSONDecodeError:
                    continue
            # Only process finished matches
            if event.get("status", {}).get("type") == "finished":
                yield event, season


def main():
    print("🔄 clean_sofascore.py — SofaScore JSON → Supabase sofascore_matches\n")
    conn = get_conn()

    for league in LEAGUES:
        print(f"🏆 {league.replace('_', ' ').title()}")
        rows = []
        for event, season in load_raw_sofascore(league):
            try:
                rows.append(parse_event(event, league, season))
            except Exception as e:
                print(f"  ⚠️  Parse error: {e}")

        if rows:
            upsert(conn, "sofascore_matches", rows, conflict_cols=["match_id", "league"])
        else:
            print(f"  ℹ️  No data found")

    conn.close()
    print("\n✅ Done")


if __name__ == "__main__":
    main()