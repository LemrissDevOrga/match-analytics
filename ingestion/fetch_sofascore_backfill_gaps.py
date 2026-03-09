"""
ingestion/fetch_sofascore_backfill_gaps.py

Fills in ANY missing match files for the current season of each league.
- Iterates through ALL 'last/N' pages until no more finished matches
- Skips matches that already have a file AND already have stats
- Fetches stats for matches that exist but are missing stats
- Safe to re-run at any time — never overwrites complete data

Run once to fix gaps, then the daily script keeps things up to date.
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

MAX_PAGES = 40  # 40 pages × ~10 matches = up to 400 matches per league (full season)


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
        r = await page.request.get(
            f"https://api.sofascore.com/api/v1/event/{match_id}/statistics"
        )
        if r.status == 200:
            return await r.json()
    except Exception as e:
        print(f"      ⚠️ Stats error for {match_id}: {e}")
    return {}


async def backfill():
    if not os.path.exists("seasons_map.json"):
        print("❌ seasons_map.json not found! Run from repo root.")
        return

    with open("seasons_map.json") as f:
        seasons_map = json.load(f)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        grand_total_new = 0
        grand_total_stats = 0

        for league_id, league_name in LEAGUES.items():
            seasons = seasons_map.get(league_id, [])
            if not seasons:
                continue

            current_season = seasons[0]
            s_id = current_season["id"]
            year = current_season["year"].replace("/", "-")
            dir_path = f"data/raw_sofascore/{league_name}/{year}"
            os.makedirs(dir_path, exist_ok=True)

            print(f"\n🏆 {league_name.replace('_',' ').title()} — {year} (season {s_id})")
            league_new = 0
            league_stats = 0
            empty_pages = 0

            for page_num in range(MAX_PAGES):
                url = (
                    f"https://api.sofascore.com/api/v1/unique-tournament/{league_id}"
                    f"/season/{s_id}/events/last/{page_num}"
                )
                try:
                    r = await page.request.get(url)
                    if r.status != 200:
                        print(f"  ⏹  Page {page_num}: HTTP {r.status} — stopping")
                        break
                    data = await r.json()
                    events = data.get("events", [])
                    if not events:
                        empty_pages += 1
                        if empty_pages >= 2:
                            break
                        continue

                    empty_pages = 0
                    finished = [
                        e for e in events
                        if e.get("status", {}).get("type") == "finished"
                    ]

                    if not finished:
                        # Page has events but none finished yet — we've gone too far back
                        # (or into future fixtures), stop
                        continue

                    page_new = 0
                    page_stats = 0

                    for event in finished:
                        m_id = event["id"]
                        mdate = match_date_str(event)
                        file_path = f"{dir_path}/{m_id}.json"

                        file_exists = os.path.exists(file_path)
                        existing_stats = {}

                        if file_exists:
                            with open(file_path) as f:
                                existing = json.load(f)
                            existing_stats = existing.get("_stats", {})

                        has_stats = bool(existing_stats)

                        if file_exists and has_stats:
                            # Already complete — skip
                            continue

                        # Need to fetch stats
                        await asyncio.sleep(0.6)
                        stats_data = await fetch_match_stats(page, m_id)
                        event["_stats"] = flatten_stats(stats_data)

                        with open(file_path, "w") as f:
                            json.dump(event, f)

                        home = event.get("homeTeam", {}).get("name", "?")
                        away = event.get("awayTeam", {}).get("name", "?")

                        if not file_exists:
                            print(f"  ✅ NEW [{mdate}] {home} vs {away}")
                            page_new += 1
                        else:
                            print(f"  📊 STATS [{mdate}] {home} vs {away}")
                            page_stats += 1

                    league_new += page_new
                    league_stats += page_stats

                    if page_new == 0 and page_stats == 0 and page_num > 5:
                        # Several pages with no gaps — likely caught up, but keep going
                        # to be thorough (don't break early)
                        pass

                    await asyncio.sleep(0.5)

                except Exception as e:
                    print(f"  ❌ Error on page {page_num}: {e}")
                    break

            print(f"  → {league_new} new matches, {league_stats} stats filled")
            grand_total_new += league_new
            grand_total_stats += league_stats

        await browser.close()

    print(f"\n✅ Backfill complete — {grand_total_new} new matches, {grand_total_stats} stats filled")
    print("\n▶️  Now run the pipeline to rebuild processed CSVs:")
    print("   python transformation/clean_sofascore.py")
    print("   python analysis/team_stats_sofascore.py")


if __name__ == "__main__":
    asyncio.run(backfill())