import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

API_KEY = os.getenv("FOOTBALL_API_KEY")
BASE_URL = "https://api.football-data.org/v4"
HEADERS = {"X-Auth-Token": API_KEY}

# La Liga competition code
COMPETITION = "PD"

# Seasons to fetch historically (year = season start year)
SEASONS = [ 2023, 2024, 2025]

def fetch_matches_for_season(season):
    url = f"{BASE_URL}/competitions/{COMPETITION}/matches?season={season}"
    response = requests.get(url, headers=HEADERS)
    
    if response.status_code == 200:
        data = response.json()
        print(f"✅ Season {season}: {len(data['matches'])} matches fetched")
        return data['matches']
    else:
        print(f"❌ Season {season} failed: {response.status_code} - {response.text}")
        return []

def save_raw(matches, season):
    os.makedirs("data/raw", exist_ok=True)
    filepath = f"data/raw/laliga_{season}.json"
    with open(filepath, "w") as f:
        json.dump(matches, f, indent=2)
    print(f"💾 Saved to {filepath}")

def main():
    for season in SEASONS:
        matches = fetch_matches_for_season(season)
        if matches:
            save_raw(matches, season)

if __name__ == "__main__":
    main()