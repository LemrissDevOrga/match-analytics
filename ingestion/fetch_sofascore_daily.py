"""
ingestion/fetch_sofascore_daily.py

Daily update script for SofaScore data.
- Always re-fetches today and yesterday's matches (overwrite) — catches late score updates
- Only fetches stats for new matches that don't have them yet
- Preserves existing stats when overwriting match metadata
"""

import json
import os
import asyncio
from datetime import datetime, timezone, timedelta
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


def match_date_str(event):
    ts = event.get("startTimestamp", 0)
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


async def fetch_match_stats(page, match_id):
    try:
        r = await page.request.get(f"https://api.sofascore.com/api/v1/event/{match_id}/statistics")
        if r.status == 200:
            return await r.json()
    except Exception as e:
        print(f"      ⚠️ Stats error for {match_id}: {e}")
    return {}


async def fetch_daily():
    if not os.path.exists("seasons_map.json"):
        print("❌ seasons_map.json not found!")
        return

    with open("seasons_map.json") as f:
        seasons_map = json.load(f)

    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    recent_dates = {today, yesterday}
    print(f"🗓️  Refreshing matches for: {yesterday} and {today}\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        total_updated = 0
        total_new_stats = 0

        for league_id, league_name in LEAGUES.items():
            seasons = seasons_map.get(league_id, [])
            if not seasons:
                continue

            current_season = seasons[0]
            s_id = current_season["id"]
            year = current_season["year"].replace("/", "-")
            dir_path = f"data/raw_sofascore/{league_name}/{year}"
            os.makedirs(dir_path, exist_ok=True)

            print(f"🏆 {league_name.replace('_',' ').title()} — {year}")
            updated = 0
            new_stats = 0

            for page_num in range(3):
                url = f"https://api.sofascore.com/api/v1/unique-tournament/{league_id}/season/{s_id}/events/last/{page_num}"
                try:
                    r = await page.request.get(url)
                    if r.status != 200:
                        break
                    data = await r.json()
                    events = data.get("events", [])
                    if not events:
                        break

                    finished = [e for e in events if e.get("status", {}).get("type") == "finished"]

                    for event in finished:
                        m_id = event["id"]
                        mdate = match_date_str(event)
                        file_path = f"{dir_path}/{m_id}.json"
                        is_new = not os.path.exists(file_path)
                        is_recent = mdate in recent_dates

                        # Skip matches that are neither new nor recent
                        if not is_new and not is_recent:
                            continue

                        # Load existing stats if file exists
                        existing_stats = {}
                        if not is_new:
                            with open(file_path) as f:
                                existing = json.load(f)
                            existing_stats = existing.get("_stats", {})

                        already_has_stats = bool(existing_stats)

                        # Fetch stats only if new match or recent match without stats
                        if is_new or (is_recent and not already_has_stats):
                            await asyncio.sleep(0.5)
                            stats_data = await fetch_match_stats(page, m_id)
                            event["_stats"] = flatten_stats(stats_data)
                            new_stats += 1
                        else:
                            # Preserve existing stats
                            event["_stats"] = existing_stats

                        # Save
                        with open(file_path, "w") as f:
                            json.dump(event, f)

                        home = event.get("homeTeam", {}).get("name", "?")
                        away = event.get("awayTeam", {}).get("name", "?")
                        tag = "✅ new" if is_new else "🔄 updated"
                        stats_tag = "📊 +stats" if (is_new or (is_recent and not already_has_stats)) else "📊 kept"
                        print(f"  {tag} {stats_tag} [{mdate}] {home} vs {away}")
                        updated += 1

                    await asyncio.sleep(0.4)

                except Exception as e:
                    print(f"  ❌ Error on page {page_num}: {e}")
                    break

            if updated == 0:
                print(f"  ℹ️  No new or recent matches")
            else:
                total_updated += updated
                total_new_stats += new_stats

        await browser.close()
        print(f"\n✅ Done — {total_updated} matches updated/added, {total_new_stats} stats fetched")


if __name__ == "__main__":
    asyncio.run(fetch_daily())