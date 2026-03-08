"""
ingestion/fetch_sofascore_daily.py

Fetches statistics for newly finished matches from SofaScore.
Runs daily via GitHub Actions — only fetches matches not yet saved.
Also fetches deep statistics (xG, shots, possession etc.) per match.
"""

import json
import os
import asyncio
from datetime import datetime, timezone
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


async def fetch_match_stats(page, match_id):
    """Fetch detailed statistics for a single match."""
    url = f"https://api.sofascore.com/api/v1/event/{match_id}/statistics"
    try:
        r = await page.request.get(url)
        if r.status == 200:
            return await r.json()
    except Exception as e:
        print(f"      ⚠️ Stats error for {match_id}: {e}")
    return {}


def flatten_stats(stats_data):
    """Flatten SofaScore statistics into a simple dict."""
    flat = {}
    for period_block in stats_data.get("statistics", []):
        if period_block.get("period") != "ALL":
            continue
        for group in period_block.get("groups", []):
            for item in group.get("statisticsItems", []):
                key = item.get("key")
                if key:
                    flat[f"home_{key}"] = item.get("homeValue")
                    flat[f"away_{key}"] = item.get("awayValue")
    return flat


async def fetch_daily():
    if not os.path.exists("seasons_map.json"):
        print("❌ seasons_map.json not found!")
        return

    with open("seasons_map.json") as f:
        seasons_map = json.load(f)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        print(f"🌐 Daily update — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")

        total_new = 0

        for league_id, league_name in LEAGUES.items():
            seasons = seasons_map.get(league_id, [])
            if not seasons:
                continue

            # Only fetch current season (first in list = most recent)
            current_season = seasons[0]
            s_id = current_season["id"]
            year = current_season["year"].replace("/", "-")

            print(f"🏆 {league_name.replace('_',' ').title()} — {year}")

            # Check last 2 pages (most recent matches)
            new_count = 0
            for page_num in [0, 1]:
                url = f"https://api.sofascore.com/api/v1/unique-tournament/{league_id}/season/{s_id}/events/last/{page_num}"
                try:
                    r = await page.request.get(url)
                    if r.status != 200:
                        break
                    data = await r.json()
                    events = data.get("events", [])
                    finished = [e for e in events if e.get("status", {}).get("type") == "finished"]

                    for event in finished:
                        m_id = event["id"]
                        dir_path = f"data/raw_sofascore/{league_name}/{year}"
                        file_path = f"{dir_path}/{m_id}.json"

                        if os.path.exists(file_path):
                            continue

                        # Fetch deep stats for new matches
                        await asyncio.sleep(0.5)
                        stats = await fetch_match_stats(page, m_id)
                        flat_stats = flatten_stats(stats)
                        event["_stats"] = flat_stats

                        os.makedirs(dir_path, exist_ok=True)
                        with open(file_path, "w") as f:
                            json.dump(event, f)

                        home = event["homeTeam"]["name"]
                        away = event["awayTeam"]["name"]
                        print(f"  ✅ {home} vs {away} (+ stats)")
                        new_count += 1
                        await asyncio.sleep(0.5)

                    await asyncio.sleep(0.4)

                except Exception as e:
                    print(f"  ❌ Error: {e}")
                    break

            if new_count == 0:
                print(f"  ℹ️  No new matches")
            total_new += new_count

        await browser.close()
        print(f"\n✅ Daily update complete — {total_new} new matches saved")


if __name__ == "__main__":
    asyncio.run(fetch_daily())