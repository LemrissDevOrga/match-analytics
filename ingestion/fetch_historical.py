import requests
import json
import os
import time
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("FOOTBALL_API_KEY")
BASE_URL = "https://api.football-data.org/v4"
HEADERS = {"X-Auth-Token": API_KEY}

LEAGUES = {
    "laliga":           {"code": "PD",  "seasons": [2023, 2024, 2025]},
    "premier_league":   {"code": "PL",  "seasons": [2023, 2024, 2025]},
    "champions_league": {"code": "CL",  "seasons": [2023, 2024, 2025]},
    "ligue1":           {"code": "FL1", "seasons": [2023, 2024, 2025]},
    "serie_a":          {"code": "SA",  "seasons": [2023, 2024, 2025]},
    "bundesliga":       {"code": "BL1", "seasons": [2023, 2024, 2025]},
    "eredivisie":       {"code": "DED", "seasons": [2023, 2024, 2025]},
    "primeira_liga":    {"code": "PPL", "seasons": [2023, 2024, 2025]},
}

def fetch_matches(competition_code, season):
    url = f"{BASE_URL}/competitions/{competition_code}/matches?season={season}"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        matches = response.json().get("matches", [])
        print(f"  ✅ Season {season}: {len(matches)} matches fetched")
        return matches
    else:
        print(f"  ❌ Season {season} failed: {response.status_code} - {response.json().get('message','')}")
        return []

def save_raw(matches, league_folder, season):
    os.makedirs(f"data/raw/{league_folder}", exist_ok=True)
    filepath = f"data/raw/{league_folder}/{league_folder}_{season}.json"
    with open(filepath, "w") as f:
        json.dump(matches, f, indent=2)
    print(f"  💾 Saved to {filepath}")

def main():
    for league_name, config in LEAGUES.items():
        print(f"\n🏆 Fetching {league_name.replace('_',' ').title()}...")
        for season in config["seasons"]:
            matches = fetch_matches(config["code"], season)
            if matches:
                save_raw(matches, league_name, season)
            print(f"  ⏳ Waiting 12 seconds...")
            time.sleep(12)

if __name__ == "__main__":
    main()