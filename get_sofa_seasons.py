import json
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

async def get_seasons():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        seasons_map = {}

        for league_id, league_name in LEAGUES.items():
            url = f"https://api.sofascore.com/api/v1/unique-tournament/{league_id}/seasons"
            response = await page.request.get(url)
            if response.status != 200:
                print(f"❌ {league_name}: {response.status}")
                continue
            data = await response.json()
            seasons = data.get("seasons", [])
            seasons_map[league_id] = seasons
            print(f"✅ {league_name}: {len(seasons)} seasons found")
            for s in seasons[:6]:
                print(f"   id={s['id']} | {s['year']}")
            await asyncio.sleep(1)

        with open("seasons_map.json", "w") as f:
            json.dump(seasons_map, f, indent=2)
        print("\n✅ Saved to seasons_map.json")
        await browser.close()

asyncio.run(get_seasons())
