"""
analysis/team_stats.py

Gold layer: reads from `matches` table → computes team stats → upserts into `team_stats`.
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


def fetch_matches(conn, league, season):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT matchday, date, home_team, home_team_tla, away_team, away_team_tla,
                   home_goals_ft, away_goals_ft, result, status
            FROM matches
            WHERE league = %s AND season = %s AND status = 'FINISHED'
            ORDER BY date
        """, (league, season))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_seasons(conn, league):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT season FROM matches WHERE league = %s", (league,))
        return [r[0] for r in cur.fetchall()]


def compute_stats(matches, league, season):
    teams = defaultdict(lambda: {
        "played": 0, "wins": 0, "draws": 0, "losses": 0,
        "goals_scored": 0, "goals_conceded": 0,
        "home_wins": 0, "home_draws": 0, "home_losses": 0,
        "away_wins": 0, "away_draws": 0, "away_losses": 0,
        "clean_sheets": 0, "tla": "",
        "results": [],  # for form
    })

    for m in matches:
        ht, at = m["home_team"], m["away_team"]
        if not ht or not at:
            continue
        hg, ag = m["home_goals_ft"] or 0, m["away_goals_ft"] or 0
        result = m["result"]

        if m.get("home_team_tla"):
            teams[ht]["tla"] = m["home_team_tla"]
        if m.get("away_team_tla"):
            teams[at]["tla"] = m["away_team_tla"]

        # Home team
        teams[ht]["played"] += 1
        teams[ht]["goals_scored"] += hg
        teams[ht]["goals_conceded"] += ag
        if ag == 0:
            teams[ht]["clean_sheets"] += 1
        if result == "HOME_TEAM":
            teams[ht]["wins"] += 1
            teams[ht]["home_wins"] += 1
            teams[ht]["results"].append(("W", m["date"]))
        elif result == "DRAW":
            teams[ht]["draws"] += 1
            teams[ht]["home_draws"] += 1
            teams[ht]["results"].append(("D", m["date"]))
        else:
            teams[ht]["losses"] += 1
            teams[ht]["home_losses"] += 1
            teams[ht]["results"].append(("L", m["date"]))

        # Away team
        teams[at]["played"] += 1
        teams[at]["goals_scored"] += ag
        teams[at]["goals_conceded"] += hg
        if hg == 0:
            teams[at]["clean_sheets"] += 1
        if result == "AWAY_TEAM":
            teams[at]["wins"] += 1
            teams[at]["away_wins"] += 1
            teams[at]["results"].append(("W", m["date"]))
        elif result == "DRAW":
            teams[at]["draws"] += 1
            teams[at]["away_draws"] += 1
            teams[at]["results"].append(("D", m["date"]))
        else:
            teams[at]["losses"] += 1
            teams[at]["away_losses"] += 1
            teams[at]["results"].append(("L", m["date"]))

    rows = []
    for team, d in teams.items():
        p = d["played"] or 1
        hp = (d["home_wins"] + d["home_draws"] + d["home_losses"]) or 1
        ap = (d["away_wins"] + d["away_draws"] + d["away_losses"]) or 1
        sorted_results = sorted(d["results"], key=lambda x: x[1] or "")
        form = "".join(r for r, _ in sorted_results[-5:])
        rows.append({
            "league":               league,
            "season":               season,
            "team":                 team,
            "played":               d["played"],
            "wins":                 d["wins"],
            "draws":                d["draws"],
            "losses":               d["losses"],
            "points":               d["wins"] * 3 + d["draws"],
            "goals_scored":         d["goals_scored"],
            "goals_conceded":       d["goals_conceded"],
            "goal_difference":      d["goals_scored"] - d["goals_conceded"],
            "home_wins":            d["home_wins"],
            "home_draws":           d["home_draws"],
            "home_losses":          d["home_losses"],
            "away_wins":            d["away_wins"],
            "away_draws":           d["away_draws"],
            "away_losses":          d["away_losses"],
            "clean_sheets":         d["clean_sheets"],
            "avg_goals_scored":     round(d["goals_scored"] / p, 2),
            "avg_goals_conceded":   round(d["goals_conceded"] / p, 2),
            "home_win_rate":        round(d["home_wins"] / hp * 100, 1),
            "away_win_rate":        round(d["away_wins"] / ap * 100, 1),
            "form_last5":           form,
        })

    return sorted(rows, key=lambda r: -r["points"])


def main():
    print("📊 team_stats.py — matches → Supabase team_stats\n")
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
            rows = compute_stats(matches, league, season)
            upsert(conn, "team_stats", rows, conflict_cols=["league", "season", "team"])
            print(f"  📅 {season} — {len(rows)} teams")

    conn.close()
    print("\n✅ Done")


if __name__ == "__main__":
    main()