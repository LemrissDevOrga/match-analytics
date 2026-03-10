"""
analysis/team_stats_sofascore.py

Gold layer: reads from `sofascore_matches` → computes advanced stats → upserts into `team_stats_advanced`.
Replaces the old CSV output in data/analytics/.
"""

import os
import sys
from collections import defaultdict

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


def fetch_matches(conn, league, season):
    import re as _re
    def _snake(k): return _re.sub(r'([A-Z])', r'_\1', k).lower()
    stat_cols = ", ".join(
        f"home_{_snake(k)}, away_{_snake(k)}" for k in STAT_KEYS
    )
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT home_team, away_team, home_goals, away_goals, result, {stat_cols}
            FROM sofascore_matches
            WHERE league = %s AND season = %s
            ORDER BY date
        """, (league, season))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_seasons(conn, league):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT season FROM sofascore_matches WHERE league = %s", (league,))
        return [r[0] for r in cur.fetchall()]


def aggregate(matches, league, season):
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
        hg = float(m.get("home_goals") or 0)
        ag = float(m.get("away_goals") or 0)
        result = m.get("result")

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

        for k in STAT_KEYS:
            import re as _re
            sk = _re.sub(r'([A-Z])', r'_\1', k).lower()
            hv = m.get(f"home_{sk}")
            av = m.get(f"away_{sk}")
            if hv is not None:
                try:
                    hv = float(hv)
                    teams[ht][f"sum_{k}"].append(hv)
                    teams[ht][f"home_sum_{k}"].append(hv)
                except (ValueError, TypeError):
                    pass
            if av is not None:
                try:
                    av = float(av)
                    teams[at][f"sum_{k}"].append(av)
                    teams[at][f"away_sum_{k}"].append(av)
                except (ValueError, TypeError):
                    pass

    rows = []
    for team, d in teams.items():
        row = {
            "league":           league,
            "season":           season,
            "team":             team,
            "played":           d["played"],
            "wins":             d["wins"],
            "draws":            d["draws"],
            "losses":           d["losses"],
            "points":           d["wins"] * 3 + d["draws"],
            "goals_scored":     int(d["goals_scored"]),
            "goals_conceded":   int(d["goals_conceded"]),
            "goal_difference":  int(d["goals_scored"] - d["goals_conceded"]),
            "home_played":      d["home_played"],
            "home_wins":        d["home_wins"],
            "away_played":      d["away_played"],
            "away_wins":        d["away_wins"],
            "home_win_rate":    round(d["home_wins"] / d["home_played"] * 100, 1) if d["home_played"] else 0,
            "away_win_rate":    round(d["away_wins"] / d["away_played"] * 100, 1) if d["away_played"] else 0,
        }
        for k in STAT_KEYS:
            import re as _re
            sk = _re.sub(r'([A-Z])', r'_\1', k).lower()
            vals = d[f"sum_{k}"]
            hvals = d[f"home_sum_{k}"]
            avals = d[f"away_sum_{k}"]
            row[f"avg_{sk}"] =       round(sum(vals)  / len(vals),  2) if vals  else None
            row[f"home_avg_{sk}"] =  round(sum(hvals) / len(hvals), 2) if hvals else None
            row[f"away_avg_{sk}"] =  round(sum(avals) / len(avals), 2) if avals else None
        rows.append(row)

    return sorted(rows, key=lambda r: -r["points"])


def main():
    print("📊 team_stats_sofascore.py — sofascore_matches → Supabase team_stats_advanced\n")
    conn = get_conn()

    for league in LEAGUES:
        seasons = fetch_seasons(conn, league)
        if not seasons:
            print(f"⚠️  No seasons found for {league}")
            continue
        print(f"\n🏆 {league.replace('_', ' ').title()}")
        for season in seasons:
            matches = fetch_matches(conn, league, season)
            if not matches:
                continue
            rows = aggregate(matches, league, season)
            upsert(conn, "team_stats_advanced", rows, conflict_cols=["league", "season", "team"])
            print(f"  📅 {season} — {len(rows)} teams")

    conn.close()
    print("\n✅ Done")


if __name__ == "__main__":
    main()