import pandas as pd
import os

LEAGUES = {
    "laliga":           [2023, 2024, 2025],
    "premier_league":   [2023, 2024, 2025],
    "champions_league": [2023, 2024, 2025],
    "ligue1":           [2023, 2024, 2025],
    "serie_a":          [2023, 2024, 2025],
    "bundesliga":       [2023, 2024, 2025],
    "eredivisie":       [2023, 2024, 2025],
    "primeira_liga":    [2023, 2024, 2025],
}

def compute_team_stats(df):
    teams = set(df["home_team"].unique()) | set(df["away_team"].unique())
    stats = []

    for team in sorted(teams):
        home = df[df["home_team"] == team].copy()
        away = df[df["away_team"] == team].copy()

        home_wins   = (home["result"] == "HOME_TEAM").sum()
        home_draws  = (home["result"] == "DRAW").sum()
        home_losses = (home["result"] == "AWAY_TEAM").sum()
        away_wins   = (away["result"] == "AWAY_TEAM").sum()
        away_draws  = (away["result"] == "DRAW").sum()
        away_losses = (away["result"] == "HOME_TEAM").sum()

        total_wins   = home_wins + away_wins
        total_draws  = home_draws + away_draws
        total_losses = home_losses + away_losses
        total_played = total_wins + total_draws + total_losses

        goals_scored   = home["home_goals_ft"].sum() + away["away_goals_ft"].sum()
        goals_conceded = home["away_goals_ft"].sum() + away["home_goals_ft"].sum()
        goal_diff      = goals_scored - goals_conceded
        points         = (total_wins * 3) + total_draws

        avg_scored   = round(goals_scored / total_played, 2) if total_played else 0
        avg_conceded = round(goals_conceded / total_played, 2) if total_played else 0

        home_cs = (home["away_goals_ft"] == 0).sum()
        away_cs = (away["home_goals_ft"] == 0).sum()

        home["date"] = pd.to_datetime(home["date"])
        away["date"] = pd.to_datetime(away["date"])
        home_res = home[["date","result"]].copy()
        home_res["team_result"] = home_res["result"].map({"HOME_TEAM":"W","DRAW":"D","AWAY_TEAM":"L"})
        away_res = away[["date","result"]].copy()
        away_res["team_result"] = away_res["result"].map({"AWAY_TEAM":"W","DRAW":"D","HOME_TEAM":"L"})
        all_res = pd.concat([home_res, away_res]).sort_values("date")
        last5   = "".join(all_res["team_result"].tail(5).tolist())

        tla = home["home_team_tla"].iloc[0] if not home.empty else away["away_team_tla"].iloc[0] if not away.empty else ""

        stats.append({
            "team": team, "home_team_tla": tla,
            "played": total_played, "wins": total_wins, "draws": total_draws, "losses": total_losses,
            "points": points, "goals_scored": int(goals_scored), "goals_conceded": int(goals_conceded),
            "goal_difference": int(goal_diff),
            "avg_goals_scored": avg_scored, "avg_goals_conceded": avg_conceded,
            "clean_sheets": int(home_cs + away_cs),
            "home_wins": int(home_wins), "home_draws": int(home_draws), "home_losses": int(home_losses),
            "away_wins": int(away_wins), "away_draws": int(away_draws), "away_losses": int(away_losses),
            "form_last5": last5,
        })

    return pd.DataFrame(stats).sort_values("points", ascending=False)

def main():
    for league, seasons in LEAGUES.items():
        print(f"\n🏆 Analyzing {league.replace('_',' ').title()}...")
        for season in seasons:
            filepath = f"data/processed/{league}/{league}_{season}.csv"
            if not os.path.exists(filepath):
                print(f"  ⚠️  Not found: {filepath}, skipping")
                continue
            df = pd.read_csv(filepath)
            df = df[df["home_goals_ft"].notna() & df["away_goals_ft"].notna()]
            if df.empty:
                print(f"  ⚠️  No data for {league} {season}")
                continue
            stats = compute_team_stats(df)
            os.makedirs(f"data/analytics/{league}", exist_ok=True)
            out = f"data/analytics/{league}/{league}_{season}_team_stats.csv"
            stats.to_csv(out, index=False)
            print(f"  💾 Saved to {out}")

if __name__ == "__main__":
    main()