import requests
import json
import os
import time
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

API_KEY = os.getenv("FOOTBALL_API_KEY")
BASE_URL = "https://api.football-data.org/v4"
HEADERS = {"X-Auth-Token": API_KEY}

LEAGUES = {
    "laliga":           "PD",
    "premier_league":   "PL",
    "champions_league": "CL",
    "ligue1":           "FL1",
    "serie_a":          "SA",
    "bundesliga":       "BL1",
}

def fetch_yesterday(competition_code):
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    today     = datetime.utcnow().strftime("%Y-%m-%d")
    url = f"{BASE_URL}/competitions/{competition_code}/matches?dateFrom={yesterday}&dateTo={today}"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        matches = response.json().get("matches", [])
        print(f"  ✅ {len(matches)} matches fetched for {yesterday}")
        return matches
    else:
        print(f"  ❌ Failed: {response.status_code} - {response.json().get('message','')}")
        return []

def save_raw(matches, league_folder):
    if not matches:
        return
    season_year = int(matches[0]["season"]["startDate"][:4])
    filepath = f"data/raw/{league_folder}/{league_folder}_{season_year}.json"
    os.makedirs(f"data/raw/{league_folder}", exist_ok=True)

    if os.path.exists(filepath):
        with open(filepath) as f:
            existing = json.load(f)
        existing_ids = {m["id"] for m in existing}
        new_matches = [m for m in matches if m["id"] not in existing_ids]
        updated = existing + new_matches
        print(f"  ➕ {len(new_matches)} new matches added to {filepath}")
    else:
        updated = matches
        print(f"  🆕 Created {filepath}")

    with open(filepath, "w") as f:
        json.dump(updated, f, indent=2)

def main():
    for league_name, code in LEAGUES.items():
        print(f"\n🏆 Daily update: {league_name.replace('_',' ').title()}...")
        matches = fetch_yesterday(code)
        if matches:
            save_raw(matches, league_name)
        print(f"  ⏳ Waiting 12 seconds...")
        time.sleep(12)

if __name__ == "__main__":
    main()