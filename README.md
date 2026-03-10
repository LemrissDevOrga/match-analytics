# ⚽ Football Analytics Platform

A multi-league European football analytics platform that automatically collects, processes, and visualises match data across 8 competitions. Built on a lakehouse architecture using GitHub as both storage and hosting.

**Live site:** [aziz1998-lemriss.github.io/match-analytics](https://aziz1998-lemriss.github.io/match-analytics/)

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Leagues Covered](#leagues-covered)
- [Repository Structure](#repository-structure)
- [Data Pipeline](#data-pipeline)
- [Season Coverage](#season-coverage)
- [Website Features](#website-features)
- [Setup & Running Locally](#setup--running-locally)
- [GitHub Actions](#github-actions)
- [Adding New Leagues or Seasons](#adding-new-leagues-or-seasons)
- [Known Limitations](#known-limitations)

---

## Overview

The platform ingests data from two sources daily, processes it through a bronze → silver → gold lakehouse pipeline, and serves it as a fully static website on GitHub Pages. No backend server, no database required at runtime — the website reads CSVs directly from the GitHub raw file CDN.

---

## Architecture

```
Raw APIs → Bronze (JSON) → Silver (CSV) → Gold (aggregated CSV) → GitHub Pages (HTML/JS)
```

### Lakehouse Layers

| Layer | Location | Format | Description |
|-------|----------|--------|-------------|
| Bronze | `data/raw/` | JSON | football-data.org match objects |
| Bronze | `data/raw_sofascore/` | JSON | SofaScore per-match event data |
| Silver | `data/processed/` | CSV | Cleaned football-data matches |
| Silver | `data/processed_sofascore/` | CSV | Cleaned SofaScore matches + stats |
| Gold | `data/analytics/` | CSV | Aggregated team stats (basic + advanced) |

---

## Data Sources

### 1. football-data.org (scores & results backbone)
- **API key:** GitHub Secret `FOOTBALL_API_KEY`
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

Champions League is flagged as `isCup: true` and gets a dedicated view (groups/knockout/stats) instead of the standard league table.

---

## Repository Structure

```
match-analytics/
├── data/
│   ├── raw/                        # Bronze: football-data.org JSON per season
│   │   └── {league}/
│   │       └── {league}_{year}.json
│   ├── raw_sofascore/              # Bronze: SofaScore JSON per match
│   │   └── {league}/
│   │       └── {SS_season}/
│   │           └── {match_id}.json
│   ├── processed/                  # Silver: cleaned football-data CSVs
│   │   └── {league}/
│   │       └── {league}_{year}.csv
│   ├── processed_sofascore/        # Silver: cleaned SofaScore CSVs
│   │   └── {league}/
│   │       └── {league}_{SS_season}.csv
│   └── analytics/                  # Gold: aggregated team stats
│       └── {league}/
│           ├── {league}_{year}_team_stats.csv      # Basic stats (football-data)
│           └── {league}_{SS_season}_advanced.csv   # Advanced stats (SofaScore)
│
├── ingestion/
│   ├── fetch_historical.py         # One-time: backfill all seasons from football-data
│   ├── fetch_daily.py              # Daily: fetch new/recent results from football-data
│   ├── fetch_sofascore_history.py  # One-time: backfill SofaScore historical seasons
│   ├── fetch_sofascore_daily.py    # Daily: fetch new SofaScore match stats
│   ├── fetch_sofascore_backfill_gaps.py  # Utility: fill any missing SofaScore matches
│   ├── fetch_fixture_ids.py        # api-sports.io fixture ID fetcher (legacy)
│   └── fetch_stats.py              # api-sports.io stats fetcher (legacy)
│
├── transformation/
│   ├── clean.py                    # Silver layer: process football-data JSON → CSV
│   └── clean_sofascore.py          # Silver layer: process SofaScore JSON → CSV
│
├── analysis/
│   ├── team_stats.py               # Gold layer: basic team stats from football-data
│   ├── team_stats_advanced.py      # Gold layer: (unused/legacy)
│   └── team_stats_sofascore.py     # Gold layer: advanced stats from SofaScore
│
├── .github/
│   └── workflows/
│       └── daily_update.yml        # GitHub Actions: runs every hour 15:00–01:00 UTC
│
├── index.html                      # Single-page frontend (all HTML/CSS/JS)
├── seasons_map.json                # SofaScore season IDs for all leagues
└── requirements.txt                # Python dependencies
```

---

## Data Pipeline

### Daily Automated Pipeline (GitHub Actions)

Runs on schedule `0 15-23,0 * * *` (every hour, 15:00–01:00 UTC):

```
1. fetch_daily.py          → data/raw/{league}/
2. clean.py                → data/processed/{league}/
3. team_stats.py           → data/analytics/{league}/*_team_stats.csv

4. fetch_sofascore_daily.py → data/raw_sofascore/{league}/{season}/
5. clean_sofascore.py       → data/processed_sofascore/{league}/
6. team_stats_sofascore.py  → data/analytics/{league}/*_advanced.csv

7. git commit & push        → triggers GitHub Pages redeploy
```

### Key File Naming Conventions

Two different season key formats are used depending on the data source:

| File type | Season key format | Example |
|-----------|-------------------|---------|
| `*_team_stats.csv` (football-data) | `{start_year}` | `laliga_2025_team_stats.csv` |
| `*_advanced.csv` (SofaScore) | `{YY}-{YY}` | `laliga_25-26_advanced.csv` |
| `processed_sofascore/` CSVs | `{YY}-{YY}` | `laliga_25-26.csv` |
| `processed/` CSVs | `{start_year}` | `laliga_2025.csv` |

The frontend maps between these using `sofaSeasons` in `LEAGUE_CONFIG`:
```js
sofaSeasons: { "2023": "23-24", "2024": "24-25", "2025": "25-26" }
```

### Silver Layer — processed CSV fields

**football-data (`processed/`):**
```
match_id, season, matchday, date, status, stage, home_team, home_team_short,
home_team_tla, away_team, away_team_short, away_team_tla,
home_goals_ft, away_goals_ft, home_goals_ht, away_goals_ht,
result, duration, referee
```
- `result` values: `HOME_TEAM`, `AWAY_TEAM`, `DRAW`
- `status` values: `FINISHED`

**SofaScore (`processed_sofascore/`):**
```
match_id, date, home_team, away_team, home_goals, away_goals, result,
home_{stat}, away_{stat}  (for each of 22 stat keys)
```
Stat keys include: `ballPossession`, `expectedGoals`, `bigChanceCreated`, `bigChanceMissed`,
`totalShotsOnGoal`, `shotsOnTarget`, `shotsOffTarget`, `blockedShots`, `goalkeeperSaves`,
`cornerKicks`, `yellowCards`, `redCards`, `totalPasses`, `accuratePasses`,
`accuratePassesPercentage`, `fouls`, `offsides`, `dribbles`, `successfulDribbles`,
`tackles`, `totalDuels`, `totalDuelsWon`

### Gold Layer — analytics CSV fields

**`*_team_stats.csv`** (football-data, basic):
```
team, played, wins, draws, losses, points, goals_scored, goals_conceded,
goal_difference, home_wins, home_draws, home_losses, away_wins, away_draws,
away_losses, clean_sheets, avg_goals_scored, avg_goals_conceded,
home_win_rate, away_win_rate, form_last5
```

**`*_advanced.csv`** (SofaScore, advanced):
```
team, played, wins, draws, losses, points, goals_scored, goals_conceded,
goal_difference, home_played, home_wins, away_played, away_wins,
home_win_rate, away_win_rate,
avg_{stat}, home_avg_{stat}, away_avg_{stat}  (for each of 22 stat keys)
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

Seasons with football-data show full functionality (league table, clean sheets, form, H2H).  
SofaScore-only seasons (21/22, 22/23) show the same UI but sourced entirely from SofaScore — clean sheets and `form_last5` are not available.

The website **dynamically discovers** available seasons at runtime by querying the GitHub contents API for `data/analytics/{league}/` — no hardcoded season lists.

---

## Website Features

The frontend is a single `index.html` file with no build step, no framework, and no server. It reads CSVs from `raw.githubusercontent.com`.

### Views

| View | Description |
|------|-------------|
| **Home** | League selector grid + links to analytics tools |
| **Overview** | League table, top scorers/defense, advanced rankings (xG/possession/shots), charts |
| **Team Detail** | KPIs, home/away split, recent results, streaks, goals trend, W/D/L doughnut, cumulative points arc |
| **Advanced Tab** | xG, possession, shots, big chances, radar vs league avg, discipline stats |
| **H2H Tab** | Head-to-head record vs any opponent across all leagues and all seasons |
| **Champions League** | Dedicated view: group/league stage table, knockout bracket, team stats |
| **Team Comparison** | Side-by-side stat comparison + radar overlay, any two teams, any league, any season |
| **Team Form** | Last 5 matches with expandable per-match SofaScore stats breakdown |

### Libraries Used (CDN only)
- [PapaParse 5.4.1](https://www.papaparse.com/) — CSV parsing
- [Chart.js 4.4.1](https://www.chartjs.org/) — all charts
- [Google Fonts](https://fonts.google.com/) — Archivo Black, Archivo, DM Mono

---

## Setup & Running Locally

### Requirements
```
python >= 3.11
playwright
playwright chromium
```

### Install
```bash
pip install -r requirements.txt
playwright install chromium
```

### Environment variables
Create a `.env` file:
```
FOOTBALL_API_KEY=your_football_data_org_key
```

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

# Historical SofaScore (all seasons)
python ingestion/fetch_sofascore_history.py

# Fill any gaps in existing SofaScore data
python ingestion/fetch_sofascore_backfill_gaps.py
```

### View the site locally
Just open `index.html` in a browser — it fetches all data directly from GitHub raw URLs, so no local server needed.

---

## GitHub Actions

The workflow file `.github/workflows/daily_update.yml` runs the full pipeline automatically.

**Schedule:** `0 15-23,0 * * *` — every hour from 15:00 to 01:00 UTC (covers match days across all European time zones).

**Required secret:** `FOOTBALL_API_KEY` (set in repo Settings → Secrets → Actions).

**What it does:**
1. Checks out the repo
2. Sets up Python 3.11 + installs dependencies + installs Playwright Chromium
3. Runs the 6-step pipeline (fetch → clean → analyse, for both data sources)
4. Commits any changed files in `data/` back to `main`
5. GitHub Pages auto-deploys on push

---

## Adding New Leagues or Seasons

### Add a new season (existing league)
Nothing to do — the website discovers seasons dynamically. Just run the pipeline and the new season's CSVs will appear automatically.

### Add a new league
1. Add the SofaScore league ID to `seasons_map.json`
2. Add the football-data code to `LEAGUES` in `ingestion/fetch_historical.py` and `fetch_daily.py`
3. Add the league to `LEAGUES` in all SofaScore ingestion scripts
4. Add the league to `LEAGUE_CONFIG` in `index.html`:
```js
my_league: {
  name: "My League", country: "Country",
  color: "#hexcolor", abbr: "ML", isCup: false,
  seasons: [],  // populated dynamically
  seasonLabels: {"2023":"23/24","2024":"24/25","2025":"25/26"},
  sofaSeasons:  {"2023":"23-24","2024":"24-25","2025":"25-26"}
}
```
5. Add a league card to the home page grid in `index.html`
6. Run backfill scripts for historical data

### Update `seasons_map.json`
SofaScore season IDs change every year. Run this to refresh:
```bash
python ingestion/fetch_sofascore_history.py --update-map-only
```
Or manually look up the new season ID on SofaScore and add it to the relevant league array in `seasons_map.json`.

---

## Known Limitations

- **SofaScore scraping:** Relies on undocumented internal API endpoints. May break if SofaScore changes their API structure or Cloudflare protection.
- **football-data free tier:** Only provides the last 2 completed seasons per league. Older seasons require SofaScore only.
- **21/22 season:** SofaScore data exists but xG is not available for that season.
- **GitHub Actions rate limits:** The hourly schedule may occasionally be delayed by GitHub's queue during peak times.
- **SofaScore team names:** Differ from football-data team names (e.g. "FC Barcelona" vs "Barcelona"). The frontend uses fuzzy matching (exact → normalised → partial) to reconcile these in the Advanced and Compare views.