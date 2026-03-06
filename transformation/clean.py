import json
import csv
import os

SEASONS = [2023, 2024, 2025]
RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

def parse_match(match):
    return {
        "match_id": match["id"],
        "season": match["season"]["startDate"][:4],
        "matchday": match["matchday"],
        "date": match["utcDate"][:10],
        "status": match["status"],
        "home_team": match["homeTeam"]["name"],
        "home_team_short": match["homeTeam"]["shortName"],
        "home_team_tla": match["homeTeam"]["tla"],
        "away_team": match["awayTeam"]["name"],
        "away_team_short": match["awayTeam"]["shortName"],
        "away_team_tla": match["awayTeam"]["tla"],
        "home_goals_ft": match["score"]["fullTime"]["home"],
        "away_goals_ft": match["score"]["fullTime"]["away"],
        "home_goals_ht": match["score"]["halfTime"]["home"],
        "away_goals_ht": match["score"]["halfTime"]["away"],
        "result": match["score"]["winner"],  # HOME_TEAM, AWAY_TEAM, DRAW
        "referee": match["referees"][0]["name"] if match["referees"] else None,
    }

def clean_season(season):
    filepath = f"{RAW_DIR}/laliga_{season}.json"
    
    with open(filepath) as f:
        matches = json.load(f)
    
    # Only keep finished matches
    finished = [m for m in matches if m["status"] == "FINISHED"]
    parsed = [parse_match(m) for m in finished]
    
    print(f"✅ Season {season}: {len(parsed)} finished matches cleaned")
    return parsed

def save_processed(rows, season):
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    filepath = f"{PROCESSED_DIR}/laliga_{season}.csv"
    
    fieldnames = rows[0].keys()
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"💾 Saved to {filepath}")

def main():
    for season in SEASONS:
        rows = clean_season(season)
        if rows:
            save_processed(rows, season)

if __name__ == "__main__":
    main()