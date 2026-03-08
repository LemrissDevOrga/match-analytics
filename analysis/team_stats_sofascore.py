"""
analysis/team_stats_sofascore.py

Builds Gold layer advanced team stats from SofaScore processed CSVs.
Output: data/analytics/{league}/{league}_{season}_advanced.csv

Per team aggregates:
  - xG for/against, shots, possession, corners, cards, passes, duels etc.
  - Home/away splits for all stats
  - Form metrics
"""

import csv
import os
from collections import defaultdict

LEAGUES = [
    "laliga", "premier_league", "champions_league",
    "bundesliga", "serie_a", "ligue1", "eredivisie", "primeira_liga"
]

STAT_KEYS = [
    "ballPossession", "expectedGoals", "bigChanceCreated", "bigChanceMissed",
    "totalShotsOnGoal", "shotsOnTarget", "shotsOffTarget", "blockedShots",
    "goalkeeperSaves", "cornerKicks", "yellowCards", "redCards",
    "totalPasses", "accuratePasses", "accuratePassesPercentage",
    "fouls", "offsides", "dribbles", "successfulDribbles",
    "tackles", "totalDuels", "totalDuelsWon",
]


def load_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def safe_float(val):
    try:
        return float(val) if val not in (None, "", "None") else None
    except (ValueError, TypeError):
        return None


def aggregate(matches):
    teams = defaultdict(lambda: {
        "played": 0, "wins": 0, "draws": 0, "losses": 0,
        "goals_scored": 0, "goals_conceded": 0,
        "home_played": 0, "home_wins": 0,
        "away_played": 0, "away_wins": 0,
        **{f"sum_{k}": [] for k in STAT_KEYS},
        **{f"home_sum_{k}": [] for k in STAT_KEYS},
        **{f"away_sum_{k}": [] for k in STAT_KEYS},
    })

    for m in matches:
        ht, at = m.get("home_team"), m.get("away_team")
        if not ht or not at:
            continue
        hg = safe_float(m.get("home_goals")) or 0
        ag = safe_float(m.get("away_goals")) or 0
        result = m.get("result")

        # Home team
        teams[ht]["played"] += 1
        teams[ht]["home_played"] += 1
        teams[ht]["goals_scored"] += hg
        teams[ht]["goals_conceded"] += ag
        if result == "HOME_TEAM":
            teams[ht]["wins"] += 1
            teams[ht]["home_wins"] += 1
        elif result == "DRAW":
            teams[ht]["draws"] += 1
        else:
            teams[ht]["losses"] += 1

        # Away team
        teams[at]["played"] += 1
        teams[at]["away_played"] += 1
        teams[at]["goals_scored"] += ag
        teams[at]["goals_conceded"] += hg
        if result == "AWAY_TEAM":
            teams[at]["wins"] += 1
            teams[at]["away_wins"] += 1
        elif result == "DRAW":
            teams[at]["draws"] += 1
        else:
            teams[at]["losses"] += 1

        # Stats
        for k in STAT_KEYS:
            hv = safe_float(m.get(f"home_{k}"))
            av = safe_float(m.get(f"away_{k}"))
            if hv is not None:
                teams[ht][f"sum_{k}"].append(hv)
                teams[ht][f"home_sum_{k}"].append(hv)
            if av is not None:
                teams[at][f"sum_{k}"].append(av)
                teams[at][f"away_sum_{k}"].append(av)

    # Build final rows
    rows = []
    for team, d in teams.items():
        p = d["played"] or 1
        row = {
            "team": team,
            "played": d["played"],
            "wins": d["wins"],
            "draws": d["draws"],
            "losses": d["losses"],
            "points": d["wins"] * 3 + d["draws"],
            "goals_scored": d["goals_scored"],
            "goals_conceded": d["goals_conceded"],
            "goal_difference": d["goals_scored"] - d["goals_conceded"],
            "home_played": d["home_played"],
            "home_wins": d["home_wins"],
            "away_played": d["away_played"],
            "away_wins": d["away_wins"],
            "home_win_rate": round(d["home_wins"] / d["home_played"] * 100, 1) if d["home_played"] else 0,
            "away_win_rate": round(d["away_wins"] / d["away_played"] * 100, 1) if d["away_played"] else 0,
        }
        # Averages for all stat keys
        for k in STAT_KEYS:
            vals = d[f"sum_{k}"]
            hvals = d[f"home_sum_{k}"]
            avals = d[f"away_sum_{k}"]
            row[f"avg_{k}"] = round(sum(vals) / len(vals), 2) if vals else None
            row[f"home_avg_{k}"] = round(sum(hvals) / len(hvals), 2) if hvals else None
            row[f"away_avg_{k}"] = round(sum(avals) / len(avals), 2) if avals else None

        rows.append(row)

    return sorted(rows, key=lambda r: -r["points"])


def save(rows, league, season):
    if not rows:
        return
    out_dir = f"data/analytics/{league}"
    os.makedirs(out_dir, exist_ok=True)
    path = f"{out_dir}/{league}_{season}_advanced.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  💾 {path} — {len(rows)} teams")


def main():
    print("📊 Building advanced team stats from SofaScore...\n")
    for league in LEAGUES:
        src_dir = f"data/processed_sofascore/{league}"
        if not os.path.exists(src_dir):
            print(f"⚠️  No SofaScore data yet for {league}")
            continue
        print(f"\n🏆 {league.replace('_',' ').title()}")
        for fname in sorted(os.listdir(src_dir), reverse=True):
            if not fname.endswith(".csv"):
                continue
            season = fname.replace(f"{league}_", "").replace(".csv", "")
            rows_in = load_csv(f"{src_dir}/{fname}")
            if not rows_in:
                continue
            rows_out = aggregate(rows_in)
            save(rows_out, league, season)


if __name__ == "__main__":
    main()