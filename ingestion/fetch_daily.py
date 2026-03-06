import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

API_KEY = os.getenv("FOOTBALL_API_KEY")
BASE_URL = "https://api.football-data.org/v4"
HEADERS = {"X-Auth-Token": API_KEY}
COMPETITION = "PD"

def fetch_yesterday_matches():
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    today = datetime.utcnow().strftime("%Y-%m-%d")
    
    url = f"{BASE_URL}/competitions/{COMPETITION}/matches?dateFrom={yesterday}&dateTo={today}"
    response = requests.get(url, headers=HEADERS)

    if response.status_code == 200:
        matches = response.json().get("matches", [])
        print(f"✅ {len(matches)} matches fetched for {yesterday}")
        return matches, yesterday
    else:
        print(f"❌ Failed: {response.status_code} - {response.text}")
        return [], yesterday

def save_raw(matches):
    os.makedirs("data/raw", exist_ok=True)
    
    if not matches:
        return

    # Determine season from first match date
    season_year = int(matches[0]["season"]["startDate"][:4])
    filepath = f"data/raw/laliga_{season_year}.json"

    # Load existing data if file exists
    if os.path.exists(filepath):
        with open(filepath) as f:
            existing = json.load(f)
        existing_ids = {m["id"] for m in existing}
        new_matches = [m for m in matches if m["id"] not in existing_ids]
        updated = existing + new_matches
        print(f"➕ {len(new_matches)} new matches added to season {season_year}")
    else:
        updated = matches
        print(f"🆕 Created new file for season {season_year}")

    with open(filepath, "w") as f:
        json.dump(updated, f, indent=2)
    print(f"💾 Saved to {filepath}")

def main():
    matches, date = fetch_yesterday_matches()
    if matches:
        save_raw(matches)