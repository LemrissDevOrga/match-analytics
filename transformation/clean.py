import json
import csv
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

def parse_match(match):
    return {
        "match_id":       match["id"],
        "season":         match["season"]["startDate"][:4],
        "matchday":       match.get("matchday"),
        "date":           match["utcDate"][:10],
        "status":         match["status"],
        "home_team":      match["homeTeam"]["name"],
        "home_team_short":match["homeTeam"]["shortName"],
        "home_team_tla":  match["homeTeam"]["tla"],
        "away_team":      match["awayTeam"]["name"],
        "away_team_short":match["awayTeam"]["shortName"],
        "away_team_tla":  match["awayTeam"]["tla"],
        "home_goals_ft":  match["score"]["fullTime"]["home"],
        "away_goals_ft":  match["score"]["fullTime"]["away"],
        "home_goals_ht":  match["score"]["halfTime"]["home"],
        "away_goals_ht":  match["score"]["halfTime"]["away"],
        "result":         match["score"]["winner"],
        "referee":        match["referees"][0]["name"] if match["referees"] else None,
    }

def clean_league_season(league, season):
    filepath = f"data/raw/{league}/{league}_{season}.json"
    if not os.path.exists(filepath):
        print(f"  ⚠️  Not found: {filepath}, skipping")
        return []

    with open(filepath) as f:
        matches = json.load(f)

    finished = [m for m in matches if m["status"] == "FINISHED"]
    parsed   = [parse_match(m) for m in finished]
    print(f"  ✅ {league} {season}: {len(parsed)} finished matches")
    return parsed

def save_processed(rows, league, season):
    os.makedirs(f"data/processed/{league}", exist_ok=True)
    filepath = f"data/processed/{league}/{league}_{season}.csv"
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  💾 Saved to {filepath}")

def main():
    for league, seasons in LEAGUES.items():
        print(f"\n🏆 Cleaning {league.replace('_',' ').title()}...")
        for season in seasons:
            rows = clean_league_season(league, season)
            if rows:
                save_processed(rows, league, season)

if __name__ == "__main__":
    main()