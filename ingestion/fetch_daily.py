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

def save_raw(matches, date):
    os.makedirs("data/raw/daily", exist_ok=True)
    filepath = f"data/raw/daily/{date}.json"
    with open(filepath, "w") as f:
        json.dump(matches, f, indent=2)
    print(f"💾 Saved to {filepath}")

def main():
    matches, date = fetch_yesterday_matches()
    if matches:
        save_raw(matches, date)

if __name__ == "__main__":
    main()