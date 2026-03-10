# ⚽ Football Analytics Platform

A multi-league European football analytics platform that automatically collects, processes, and visualises match data across 8 competitions. Built on a medallion lakehouse architecture with Supabase as the database and GitHub Pages for hosting.

**Live site:** [aziz1998-lemriss.github.io/match-analytics](https://aziz1998-lemriss.github.io/match-analytics/)

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Leagues Covered](#leagues-covered)
- [Repository Structure](#repository-structure)
- [Database Schema](#database-schema)
- [Data Pipeline](#data-pipeline)
- [Season Coverage](#season-coverage)
- [Website Features](#website-features)
- [Setup & Running Locally](#setup--running-locally)
- [GitHub Actions](#github-actions)
- [Adding New Leagues or Seasons](#adding-new-leagues-or-seasons)
- [Known Limitations](#known-limitations)

---

## Overview

The platform ingests data from two sources daily, processes it through a bronze → silver → gold medallion pipeline, and persists everything in Supabase (PostgreSQL). The frontend is a fully static single-page app on GitHub Pages that queries Supabase directly via the JS client — no CSV files, no backend server at runtime.

---

## Architecture

```
Raw APIs → Bronze (JSON) → Supabase Silver (matches / sofascore_matches)
                         → Supabase Gold  (team_stats / team_stats_advanced)
                         → GitHub Pages (index.html queries Supabase JS client)
```

### Medallion Layers

| Layer | Location | Format | Description |
|-------|----------|--------|-------------|
| Bronze | `data/raw/` | JSON | football-data.org match objects (kept in repo) |
| Bronze | `data/raw_sofascore/` | JSON | SofaScore per-match event data (kept in repo) |
| Silver | Supabase `matches` | PostgreSQL | Cleaned football-data matches |
| Silver | Supabase `sofascore_matches` | PostgreSQL | Cleaned SofaScore matches + per-match stats |
| Gold | Supabase `team_stats` | PostgreSQL | Basic aggregated team stats (football-data) |
| Gold | Supabase `team_stats_advanced` | PostgreSQL | Advanced aggregated team stats (SofaScore) |

Raw JSON files are the only files committed to the repo. All processed and aggregated data lives exclusively in Supabase.

---

## Data Sources

### 1. football-data.org (scores & results backbone)
- **API key:** `.env` as `FOOTBALL_API_KEY`, GitHub Secret `FOOTBALL_API_KEY`
- **Coverage:** Last 2 seasons per league (free tier)
- **Provides:** Match results, scores, dates, referee, stage/round
- **Limitation:** No xG, no shots, no possession stats

### 2. SofaScore (deep stats — scraped via Playwright)
- **No official API** — uses `page.request.get()` through Playwright/Chromium to bypass Cloudflare
- **Coverage:** 5 seasons back (21/22 → present); 21/22 has no xG
- **Provides:** xG, shots, possession, corners, cards, passes, duels, big chances, fouls, offsides, dribbles
- **Season IDs:** Stored in `seasons_map.json` (keyed by SofaScore league ID)

---

## Leagues Covered

| League | football-data code | SofaScore ID | Folder | Color |
|--------|-------------------|--------------|--------|-------|
| Premier League | PL | 17 | `premier_league` | `#7b2fb5` |
| La Liga | PD | 8 | `laliga` | `#ee8700` |
| Champions League | CL | 7 | `champions_league` | `#1a8fff` |
| Bundesliga | BL1 | 35 | `bundesliga` | `#d0021b` |
| Serie A | SA | 23 | `serie_a` | `#007bc3` |
| Ligue 1 | FL1 | 34 | `ligue1` | `#4466ff` |
| Eredivisie | DED | 37 | `eredivisie` | `#ff6b00` |
| Primeira Liga | PPL | 238 | `primeira_liga` | `#006600` |

Champions League is flagged as `isCup: true` and gets a dedicated view (group/league stage table, knockout bracket, team stats) instead of the standard league table.

---

## Repository Structure

```
match-analytics/
├── data/
│   ├── raw/                        # Bronze: football-data.org JSON per season
│   │   └── {league}/
│   │       └── {league}_{year}.json
│   └── raw_sofascore/              # Bronze: SofaScore JSON per match
│       └── {league}/
│           └── {SS_season}/
│               └── {match_id}.json
│
├── ingestion/
│   ├── fetch_historical.py         # One-time: backfill all seasons from football-data
│   ├── fetch_daily.py              # Daily: fetch new/recent results from football-data
│   ├── fetch_sofascore_history.py  # One-time: backfill SofaScore historical seasons
│   ├── fetch_sofascore_daily.py    # Daily: fetch new SofaScore match stats
│   └── fetch_sofascore_backfill_gaps.py  # Utility: fill any missing SofaScore matches
│
├── transformation/
│   ├── clean.py                    # Silver layer: football-data JSON → Supabase matches
│   └── clean_sofascore.py          # Silver layer: SofaScore JSON → Supabase sofascore_matches
│
├── analysis/
│   ├── team_stats.py               # Gold layer: matches → Supabase team_stats
│   └── team_stats_sofascore.py     # Gold layer: sofascore_matches → Supabase team_stats_advanced
│
├── scripts/
│   └── db.py                       # Shared psycopg2 connection + bulk upsert helper
│
├── sql/
│   └── schema.sql                  # Supabase table definitions (run once)
│
├── .github/
│   └── workflows/
│       └── daily_update.yml        # GitHub Actions: runs every hour 15:00–01:00 UTC
│
├── config.js                       # Generated at build time by CI (gitignored locally)
├── index.html                      # Single-page frontend (all HTML/CSS/JS)
├── seasons_map.json                # SofaScore season IDs for all leagues
└── requirements.txt                # Python dependencies
```

---

## Database Schema

Four tables in Supabase (all in the `public` schema, RLS enabled with public read access):

### `matches` — Silver layer (football-data.org)
```
match_id, league, season, matchday, date, status, stage,
home_team, home_team_short, home_team_tla,
away_team, away_team_short, away_team_tla,
home_goals_ft, away_goals_ft, home_goals_ht, away_goals_ht,
result, duration, referee
```
- `season` format: start year e.g. `"2024"` for the 24/25 season
- `result` values: `HOME_TEAM`, `AWAY_TEAM`, `DRAW`
- Unique constraint: `(match_id, league)`

### `sofascore_matches` — Silver layer (SofaScore)
```
match_id, league, season, date, home_team, away_team,
home_goals, away_goals, result,
home_{stat}, away_{stat}  (22 stat columns each, all snake_case)
```
- `season` format: `"YY-YY"` e.g. `"25-26"`
- Stat columns (snake_case): `ball_possession`, `expected_goals`, `big_chance_created`, `big_chance_missed`, `total_shots_on_goal`, `shots_on_target`, `shots_off_target`, `blocked_shots`, `goalkeeper_saves`, `corner_kicks`, `yellow_cards`, `red_cards`, `total_passes`, `accurate_passes`, `accurate_passes_percentage`, `fouls`, `offsides`, `dribbles`, `successful_dribbles`, `tackles`, `total_duels`, `total_duels_won`
- Unique constraint: `(match_id, league)`

### `team_stats` — Gold layer (football-data.org)
```
league, season, team, played, wins, draws, losses, points,
goals_scored, goals_conceded, goal_difference,
home_wins, home_draws, home_losses, away_wins, away_draws, away_losses,
clean_sheets, avg_goals_scored, avg_goals_conceded,
home_win_rate, away_win_rate, form_last5
```
- `season` format: start year e.g. `"2024"`
- Unique constraint: `(league, season, team)`

### `team_stats_advanced` — Gold layer (SofaScore)
```
league, season, team, played, wins, draws, losses, points,
goals_scored, goals_conceded, goal_difference,
home_played, home_wins, away_played, away_wins,
home_win_rate, away_win_rate,
avg_{stat}, home_avg_{stat}, away_avg_{stat}  (for each of 22 stat keys)
```
- `season` format: `"YY-YY"` e.g. `"25-26"`
- Unique constraint: `(league, season, team)`

---

## Data Pipeline

### Daily Automated Pipeline (GitHub Actions)

Runs on schedule `0 15-23,0 * * *` (every hour, 15:00–01:00 UTC):

```
1. fetch_daily.py              → data/raw/{league}/
2. clean.py                    → Supabase: matches (upsert)
3. team_stats.py               → Supabase: team_stats (upsert)

4. fetch_sofascore_daily.py    → data/raw_sofascore/{league}/{season}/
5. clean_sofascore.py          → Supabase: sofascore_matches (upsert)
6. team_stats_sofascore.py     → Supabase: team_stats_advanced (upsert)

7. Generate config.js          → injects SUPABASE_URL + SUPABASE_ANON_KEY from secrets
8. git commit & push           → raw JSON + config.js → triggers GitHub Pages redeploy
```

All upserts use `ON CONFLICT DO UPDATE` so reruns are safe and idempotent.

### Season Key Conventions

Two season key formats are used depending on the data source:

| Table | Season key format | Example |
|-------|-------------------|---------|
| `matches`, `team_stats` | `{start_year}` | `"2024"` for 24/25 season |
| `sofascore_matches`, `team_stats_advanced` | `{YY}-{YY}` | `"24-25"` for 24/25 season |

The frontend maps between these using `sofaSeasons` in `LEAGUE_CONFIG`:
```js
sofaSeasons: { "2023": "23-24", "2024": "24-25", "2025": "25-26" }
```

---

## Season Coverage

| Season | football-data | SofaScore | xG available |
|--------|--------------|-----------|--------------|
| 21/22  | ❌ | ✅ | ❌ |
| 22/23  | ❌ | ✅ | ✅ |
| 23/24  | ✅ | ✅ | ✅ |
| 24/25  | ✅ | ✅ | ✅ |
| 25/26  | ✅ | ✅ | ✅ |

Seasons with football-data show full functionality (league table, clean sheets, form, H2H). SofaScore-only seasons (21/22, 22/23) show the same UI but sourced entirely from `team_stats_advanced` — clean sheets and `form_last5` are not available and are hidden gracefully.

The website **dynamically discovers** available seasons at runtime by querying distinct seasons from Supabase — no hardcoded season lists in the frontend.

---

## Website Features

The frontend is a single `index.html` file with no build step, no framework, and no server. It queries Supabase directly via the `@supabase/supabase-js` client. Credentials are injected at build time via `config.js` (generated by GitHub Actions from repository secrets — never hardcoded).

### Views

| View | Description |
|------|-------------|
| **Home** | League selector grid |
| **Overview** | League table, top scorers/defense, advanced rankings (xG/possession/shots), charts |
| **Team Detail** | KPIs, home/away split, recent results, streaks, goals trend, W/D/L doughnut, cumulative points arc |
| **Advanced Tab** | xG, possession, shots, big chances, radar vs league avg, discipline stats |
| **H2H Tab** | Head-to-head record vs any opponent across all leagues and all seasons |
| **Champions League** | Dedicated view: group/league stage table, knockout bracket, team stats |
| **Team Comparison** | Side-by-side stat comparison + radar overlay, any two teams, any league, any season |
| **Team Form** | Last 5 matches with expandable per-match SofaScore stats breakdown |

### Libraries Used (CDN only)
- [@supabase/supabase-js v2](https://supabase.com/docs/reference/javascript) — database client
- [Chart.js 4.4.1](https://www.chartjs.org/) — all charts
- [Google Fonts](https://fonts.google.com/) — Archivo Black, Archivo, DM Mono

---

## Setup & Running Locally

### Requirements
```
python >= 3.11
psycopg2-binary
python-dotenv
playwright
```

### Install
```bash
pip install -r requirements.txt
playwright install chromium
```

### Environment variables
Create a `.env` file at the repo root:
```
FOOTBALL_API_KEY=your_football_data_org_key
DATABASE_URL=postgresql://postgres.[ref]:[password]@aws-0-xx-xxx.pooler.supabase.com:6543/postgres
```

`DATABASE_URL` uses the **Transaction Pooler** connection string from Supabase → Settings → Database (not the direct connection — that requires a paid plan).

### Supabase setup (first time only)
Run `sql/schema.sql` in the Supabase SQL Editor to create all 4 tables, indexes, and RLS policies.

### Run the full pipeline manually
```bash
# Football-data pipeline
python ingestion/fetch_daily.py
python transformation/clean.py
python analysis/team_stats.py

# SofaScore pipeline
python ingestion/fetch_sofascore_daily.py
python transformation/clean_sofascore.py
python analysis/team_stats_sofascore.py
```

### Backfill from scratch
```bash
# Historical football-data (all seasons)
python ingestion/fetch_historical.py
python transformation/clean.py
python analysis/team_stats.py

# Historical SofaScore (all seasons)
python ingestion/fetch_sofascore_history.py
python transformation/clean_sofascore.py
python analysis/team_stats_sofascore.py

# Fill any gaps in SofaScore data
python ingestion/fetch_sofascore_backfill_gaps.py
python transformation/clean_sofascore.py
python analysis/team_stats_sofascore.py
```

### View the site locally
Create a `config.js` at the repo root (it's gitignored):
```js
window.SUPABASE_URL = "https://xxxx.supabase.co";
window.SUPABASE_ANON_KEY = "eyJ...";
```
Then open `index.html` directly in a browser.

---

## GitHub Actions

The workflow `.github/workflows/daily_update.yml` runs the full pipeline automatically.

**Schedule:** `0 15-23,0 * * *` — every hour from 15:00 to 01:00 UTC.

**Required secrets** (Settings → Secrets → Actions):

| Secret | Description |
|--------|-------------|
| `FOOTBALL_API_KEY` | football-data.org API key |
| `DATABASE_URL` | Supabase Transaction Pooler connection string |
| `SUPABASE_URL` | Supabase project URL e.g. `https://xxxx.supabase.co` |
| `SUPABASE_ANON_KEY` | Supabase anon/public key (starts with `eyJ`) |

**What it does:**
1. Checks out the repo
2. Sets up Python 3.11 + installs dependencies + installs Playwright Chromium
3. Runs the 6-step pipeline (fetch → transform → analyse, for both data sources)
4. Generates `config.js` from secrets
5. Commits raw JSON + `config.js` back to `main`
6. GitHub Pages auto-deploys on push

---

## Adding New Leagues or Seasons

### Add a new season (existing league)
Nothing to do — the website discovers seasons dynamically by querying distinct seasons from Supabase. Just run the pipeline and data appears automatically.

### Add a new league
1. Add the SofaScore league ID to `seasons_map.json`
2. Add the football-data code to `LEAGUES` in `ingestion/fetch_historical.py` and `fetch_daily.py`
3. Add the league to `LEAGUES` in all SofaScore ingestion scripts
4. Add the league to `LEAGUE_CONFIG` in `index.html`:
```js
my_league: {
  name: "My League", country: "Country",
  color: "#hexcolor", abbr: "ML", isCup: false,
  seasons: [],  // populated dynamically at runtime
  seasonLabels: {"2023":"23/24","2024":"24/25","2025":"25/26"},
  sofaSeasons:  {"2023":"23-24","2024":"24-25","2025":"25-26"}
}
```
5. Add a league card to the home page grid in `index.html`
6. Run backfill scripts for historical data

### Update `seasons_map.json`
SofaScore season IDs change every year. Manually look up the new season ID on SofaScore and add it to the relevant league array in `seasons_map.json`.

---

## Known Limitations

- **SofaScore scraping:** Relies on undocumented internal API endpoints. May break if SofaScore changes their API structure or Cloudflare protection.
- **football-data free tier:** Only provides the last 2 completed seasons per league. Older seasons are SofaScore-only.
- **21/22 season:** SofaScore data exists but xG is not available.
- **GitHub Actions rate limits:** The hourly schedule may occasionally be delayed by GitHub's queue during peak times.
- **SofaScore team names:** Differ from football-data team names (e.g. "FC Barcelona" vs "Barcelona"). The frontend uses fuzzy matching to reconcile these in the Advanced and Compare views.
- **Supabase free tier:** 500MB database storage and 2GB bandwidth per month. May need upgrading as historical data grows.
