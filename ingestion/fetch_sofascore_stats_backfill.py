"""
ingestion/fetch_sofascore_stats_backfill.py

For matches already saved by fetch_sofascore_history.py that don't have
deep stats yet (_stats field), this script fetches and adds them.

Run after fetch_sofascore_history.py completes.
Can be interrupted and resumed — skips matches that already have _stats.
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


def flatten_stats(stats_data):
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


def count_pending():
    total = 0
    for league_name in LEAGUES.values():
        league_dir = f"data/raw_sofascore/{league_name}"
        if not os.path.exists(league_dir):
            continue
        for season_dir in os.listdir(league_dir):
            for fname in os.listdir(f"{league_dir}/{season_dir}"):
                fpath = f"{league_dir}/{season_dir}/{fname}"
                with open(fpath) as f:
                    data = json.load(f)
                if "_stats" not in data:
                    total += 1
    return total


async def backfill_stats():
    pending = count_pending()
    print(f"📊 Found {pending} matches without stats\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        done = 0
        errors = 0

        for league_name in LEAGUES.values():
            league_dir = f"data/raw_sofascore/{league_name}"
            if not os.path.exists(league_dir):
                continue

            for season_dir in sorted(os.listdir(league_dir), reverse=True):
                season_path = f"{league_dir}/{season_dir}"
                files = os.listdir(season_path)
                season_pending = 0

                for fname in files:
                    fpath = f"{season_path}/{fname}"
                    with open(fpath) as f:
                        event = json.load(f)
                    if "_stats" in event:
                        continue
                    season_pending += 1

                if season_pending == 0:
                    continue

                print(f"\n🏆 {league_name} {season_dir} — {season_pending} matches need stats")

                for fname in files:
                    fpath = f"{season_path}/{fname}"
                    with open(fpath) as f:
                        event = json.load(f)

                    if "_stats" in event:
                        continue

                    m_id = event["id"]
                    home = event.get("homeTeam", {}).get("name", "?")
                    away = event.get("awayTeam", {}).get("name", "?")

                    try:
                        url = f"https://api.sofascore.com/api/v1/event/{m_id}/statistics"
                        r = await page.request.get(url)
                        if r.status == 200:
                            stats_data = await r.json()
                            event["_stats"] = flatten_stats(stats_data)
                        else:
                            event["_stats"] = {}  # mark as attempted

                        with open(fpath, "w") as f:
                            json.dump(event, f)

                        done += 1
                        print(f"  ✅ [{done}/{pending}] {home} vs {away}")

                    except Exception as e:
                        errors += 1
                        print(f"  ❌ {home} vs {away}: {e}")

                    await asyncio.sleep(0.4)

        await browser.close()
        print(f"\n✅ Done! {done} matches updated, {errors} errors")


if __name__ == "__main__":
    asyncio.run(backfill_stats())