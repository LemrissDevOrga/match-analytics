"""
transformation/clean_sofascore.py

Transforms raw SofaScore JSON match files into clean CSVs.
Output: data/processed_sofascore/{league}/{league}_{season}.csv

Each row = one match with all available stats flattened.
"""

import json
import csv
import os
from datetime import datetime

LEAGUES = {
    "8":   "laliga",
    "17":  "premier_league",
    "7":   "champions_league",
    "35":  "bundesliga",
    "23":  "serie_a",
    "34":  "ligue1",
    "37":  "eredivisie",
    "238": "primeira_liga",
}

# All known stat keys from SofaScore statistics
STAT_KEYS = [
    "ballPossession", "expectedGoals", "bigChanceCreated", "bigChanceMissed",
    "totalShotsOnGoal", "shotsOnTarget", "shotsOffTarget", "blockedShots",
    "goalkeeperSaves", "cornerKicks", "freeKicks", "yellowCards", "redCards",
    "totalPasses", "accuratePasses", "accuratePassesPercentage",
    "totalLongBalls", "accurateLongBalls", "totalCross", "accurateCross",
    "dribbles", "successfulDribbles", "tackles", "totalDuels", "totalDuelsWon",
    "fouls", "offsides", "throwIns", "goalKicks",
]


def parse_score(event):
    """Extract final score handling ET and penalties."""
    home_score = event.get("homeScore", {})
    away_score = event.get("awayScore", {})

    # Use normaltime if available (excludes ET and pens)
    # Otherwise fall back to current (full time)
    home_goals = home_score.get("normaltime", home_score.get("current", 0)) or 0
    away_goals = away_score.get("normaltime", away_score.get("current", 0)) or 0

    # Check if went to ET or penalties
    home_et = home_score.get("overtime")
    away_et = away_score.get("overtime")
    home_pen = home_score.get("penalties")
    away_pen = away_score.get("penalties")

    if home_et is not None:
        home_goals = home_score.get("normaltime", 0) + (home_et or 0)
        away_goals = away_score.get("normaltime", 0) + (away_et or 0)

    duration = "REGULAR"
    if home_pen is not None:
        duration = "PENALTY_SHOOTOUT"
    elif home_et is not None:
        duration = "EXTRA_TIME"

    winner = None
    if home_goals > away_goals:
        winner = "HOME_TEAM"
    elif away_goals > home_goals:
        winner = "AWAY_TEAM"
    else:
        winner = "DRAW"

    return home_goals, away_goals, duration, winner, home_pen, away_pen


def parse_event(event):
    home_goals, away_goals, duration, winner, home_pen, away_pen = parse_score(event)

    row = {
        "match_id":        event.get("id"),
        "date":            datetime.fromtimestamp(event.get("startTimestamp", 0)).strftime("%Y-%m-%d"),
        "season":          event.get("season", {}).get("year", ""),
        "round":           event.get("roundInfo", {}).get("round"),
        "status":          event.get("status", {}).get("type", ""),
        "home_team":       event.get("homeTeam", {}).get("name", ""),
        "home_team_short": event.get("homeTeam", {}).get("shortName", ""),
        "home_team_slug":  event.get("homeTeam", {}).get("slug", ""),
        "away_team":       event.get("awayTeam", {}).get("name", ""),
        "away_team_short": event.get("awayTeam", {}).get("shortName", ""),
        "away_team_slug":  event.get("awayTeam", {}).get("slug", ""),
        "home_goals":      home_goals,
        "away_goals":      away_goals,
        "duration":        duration,
        "result":          winner,
        "home_penalties":  home_pen,
        "away_penalties":  away_pen,
        "has_xg":          event.get("hasXg", False),
    }

    # Flatten _stats if available
    stats = event.get("_stats", {})
    for key in STAT_KEYS:
        row[f"home_{key}"] = stats.get(f"home_{key}")
        row[f"away_{key}"] = stats.get(f"away_{key}")

    return row


def clean_league_season(league_name, season_dir):
    src_dir = f"data/raw_sofascore/{league_name}/{season_dir}"
    if not os.path.exists(src_dir):
        return []

    rows = []
    for fname in sorted(os.listdir(src_dir)):
        if not fname.endswith(".json"):
            continue
        with open(f"{src_dir}/{fname}") as f:
            event = json.load(f)
        if event.get("status", {}).get("type") != "finished":
            continue
        try:
            rows.append(parse_event(event))
        except Exception as e:
            print(f"    ⚠️ Error parsing {fname}: {e}")

    return rows


def save_csv(rows, league_name, season_year):
    if not rows:
        return
    out_dir = f"data/processed_sofascore/{league_name}"
    os.makedirs(out_dir, exist_ok=True)
    safe_year = season_year.replace("/", "-")
    out_path = f"{out_dir}/{league_name}_{safe_year}.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    stats_count = sum(1 for r in rows if r.get("home_expectedGoals") is not None)
    print(f"  💾 {out_path} — {len(rows)} matches ({stats_count} with xG stats)")


def main():
    print("🧹 Transforming SofaScore data...\n")
    for league_name in LEAGUES.values():
        league_dir = f"data/raw_sofascore/{league_name}"
        if not os.path.exists(league_dir):
            continue
        print(f"\n🏆 {league_name.replace('_',' ').title()}")
        for season_dir in sorted(os.listdir(league_dir), reverse=True):
            rows = clean_league_season(league_name, season_dir)
            if rows:
                save_csv(rows, league_name, season_dir)
            else:
                print(f"  ⚠️  No finished matches in {season_dir}")


if __name__ == "__main__":
    main()