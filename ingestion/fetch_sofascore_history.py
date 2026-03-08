"""
ingestion/fetch_sofascore_history.py

Fetches last 5 seasons of match events from SofaScore for all leagues.
Saves raw JSON per match to data/raw_sofascore/{league}/{season_year}/{match_id}.json

Run once (or resume anytime — skips already-fetched matches).
Uses Playwright's browser network stack to bypass Cloudflare.
"""

import json
import os
import asyncio
from playwright.async_api import async_playwright

LEAGUES = {
    "8":   "laliga",
    "17":  "premier_league",
    "7":   "champions_league",
    "35":  "bundesliga",
    "23":  "serie_a",
    "34":  "ligue1",
    "37":  "eredivisie",
    "238": "primeira_liga",
}

# How many seasons back to fetch (5 = last 5 seasons)
SEASONS_TO_FETCH = 5


async def fetch_history():
    if not os.path.exists("seasons_map.json"):
        print("❌ seasons_map.json not found! Run get_sofa_seasons.py first.")
        return

    with open("seasons_map.json") as f:
        seasons_map = json.load(f)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        print("🌐 Browser started\n")

        for league_id, league_name in LEAGUES.items():
            seasons = seasons_map.get(league_id, [])[:SEASONS_TO_FETCH]
            if not seasons:
                print(f"⚠️  No seasons found for {league_name}")
                continue

            print(f"\n{'='*50}")
            print(f"🏆 {league_name.replace('_',' ').title()} ({len(seasons)} seasons)")
            print(f"{'='*50}")

            for season in seasons:
                s_id = season["id"]
                year = season["year"].replace("/", "-")
                page_num = 0
                total_saved = 0
                total_skipped = 0

                print(f"\n  📅 Season {year} (id={s_id})")

                while True:
                    url = f"https://api.sofascore.com/api/v1/unique-tournament/{league_id}/season/{s_id}/events/last/{page_num}"

                    try:
                        response = await page.request.get(url)

                        if response.status != 200:
                            print(f"    🏁 No more pages at page {page_num} (status {response.status})")
                            break

                        data = await response.json()
                        events = data.get("events", [])

                        if not events:
                            print(f"    🏁 No events on page {page_num}")
                            break

                        # Only process finished matches
                        finished = [e for e in events if e.get("status", {}).get("type") == "finished"]
                        print(f"    📄 Page {page_num}: {len(events)} events ({len(finished)} finished)")

                        for event in finished:
                            m_id = event["id"]
                            dir_path = f"data/raw_sofascore/{league_name}/{year}"
                            file_path = f"{dir_path}/{m_id}.json"

                            if os.path.exists(file_path):
                                total_skipped += 1
                                continue

                            os.makedirs(dir_path, exist_ok=True)
                            with open(file_path, "w") as f:
                                json.dump(event, f)
                            total_saved += 1

                        page_num += 1
                        await asyncio.sleep(0.4)

                    except Exception as e:
                        print(f"    ❌ Error on page {page_num}: {e}")
                        break

                print(f"  ✅ Season {year}: saved={total_saved}, skipped={total_skipped}")

        await browser.close()
        print("\n\n✅ Done! All historical match events saved.")
        print_progress()


def print_progress():
    print("\n=== PROGRESS ===")
    total_matches = 0
    for league_name in LEAGUES.values():
        league_dir = f"data/raw_sofascore/{league_name}"
        if not os.path.exists(league_dir):
            continue
        league_total = 0
        for season_dir in os.listdir(league_dir):
            count = len(os.listdir(f"{league_dir}/{season_dir}"))
            league_total += count
            print(f"  {league_name} {season_dir}: {count} matches")
        total_matches += league_total
    print(f"\n  Total: {total_matches} matches saved")


if __name__ == "__main__":
    asyncio.run(fetch_history())