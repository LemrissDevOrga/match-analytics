import pandas as pd
import json
import os

PROCESSED_DIR = "data/processed"
ANALYTICS_DIR = "data/analytics"
SEASONS = [2023, 2024, 2025]

def load_season(season):
    return pd.read_csv(f"{PROCESSED_DIR}/laliga_{season}.csv")

def compute_team_stats(df):
    teams = set(df["home_team"].unique()) | set(df["away_team"].unique())
    stats = []

    for team in sorted(teams):
        home = df[df["home_team"] == team].copy()
        away = df[df["away_team"] == team].copy()

        # Wins, draws, losses
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

        # Goals
        goals_scored    = home["home_goals_ft"].sum() + away["away_goals_ft"].sum()
        goals_conceded  = home["away_goals_ft"].sum() + away["home_goals_ft"].sum()
        goal_difference = goals_scored - goals_conceded
        points          = (total_wins * 3) + total_draws

        # Averages
        avg_scored   = round(goals_scored / total_played, 2) if total_played else 0
        avg_conceded = round(goals_conceded / total_played, 2) if total_played else 0

        # Clean sheets
        home_cs = (home["away_goals_ft"] == 0).sum()
        away_cs = (away["home_goals_ft"] == 0).sum()
        clean_sheets = home_cs + away_cs

        # Form: last 5 matches by date
        home["date"] = pd.to_datetime(home["date"])
        away["date"] = pd.to_datetime(away["date"])

        home_results = home[["date", "result"]].copy()
        home_results["team_result"] = home_results["result"].map(
            {"HOME_TEAM": "W", "DRAW": "D", "AWAY_TEAM": "L"}
        )
        away_results = away[["date", "result"]].copy()
        away_results["team_result"] = away_results["result"].map(
            {"AWAY_TEAM": "W", "DRAW": "D", "HOME_TEAM": "L"}
        )

        all_results = pd.concat([home_results, away_results]).sort_values("date")
        last5 = "".join(all_results["team_result"].tail(5).tolist())

        stats.append({
            "team": team,
            "played": total_played,
            "wins": total_wins,
            "draws": total_draws,
            "losses": total_losses,
            "points": points,
            "goals_scored": goals_scored,
            "goals_conceded": goals_conceded,
            "goal_difference": goal_difference,
            "avg_goals_scored": avg_scored,
            "avg_goals_conceded": avg_conceded,
            "clean_sheets": clean_sheets,
            "home_wins": home_wins,
            "home_draws": home_draws,
            "home_losses": home_losses,
            "away_wins": away_wins,
            "away_draws": away_draws,
            "away_losses": away_losses,
            "form_last5": last5,
        })

    return pd.DataFrame(stats).sort_values("points", ascending=False)

def main():
    os.makedirs(ANALYTICS_DIR, exist_ok=True)

    for season in SEASONS:
        print(f"\n📊 Analyzing season {season}...")
        df = load_season(season)
        stats = compute_team_stats(df)
        
        output_path = f"{ANALYTICS_DIR}/laliga_{season}_team_stats.csv"
        stats.to_csv(output_path, index=False)
        print(stats[["team", "played", "wins", "draws", "losses", "points", "goals_scored", "goal_difference", "form_last5"]].to_string(index=False))
        print(f"\n💾 Saved to {output_path}")

if __name__ == "__main__":
    main()