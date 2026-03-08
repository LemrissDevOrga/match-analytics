import json
import asyncio
from playwright.async_api import async_playwright

async def check_match():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Get one match from La Liga 24/25
        url = "https://api.sofascore.com/api/v1/unique-tournament/8/season/61643/events/last/0"
        r = await page.request.get(url)
        data = await r.json()
        match = data['events'][0]
        match_id = match['id']

        print("=== MATCH EVENT FIELDS ===")
        print(json.dumps(list(match.keys()), indent=2))
        print(f"\nMatch: {match['homeTeam']['name']} vs {match['awayTeam']['name']}")

        # Get statistics
        await asyncio.sleep(1)
        r2 = await page.request.get(f"https://api.sofascore.com/api/v1/event/{match_id}/statistics")
        stats = await r2.json()
        print("\n=== STATISTICS STRUCTURE ===")
        print(json.dumps(stats, indent=2)[:2000])

        await browser.close()

asyncio.run(check_match())
