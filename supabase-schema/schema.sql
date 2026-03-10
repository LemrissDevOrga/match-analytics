-- ============================================================
-- Football Analytics Platform — Supabase Schema
-- Run this once in the Supabase SQL editor
-- ============================================================

-- Drop existing tables first (clean slate)
DROP TABLE IF EXISTS sofascore_matches CASCADE;
DROP TABLE IF EXISTS team_stats_advanced CASCADE;
DROP TABLE IF EXISTS team_stats CASCADE;
DROP TABLE IF EXISTS matches CASCADE;

-- ── 1. MATCHES ───────────────────────────────────────────────
CREATE TABLE matches (
    id              BIGSERIAL PRIMARY KEY,
    match_id        TEXT        NOT NULL,
    league          TEXT        NOT NULL,
    season          TEXT        NOT NULL,
    matchday        INTEGER,
    date            DATE,
    status          TEXT,
    stage           TEXT,
    home_team       TEXT,
    home_team_short TEXT,
    home_team_tla   TEXT,
    away_team       TEXT,
    away_team_short TEXT,
    away_team_tla   TEXT,
    home_goals_ft   INTEGER,
    away_goals_ft   INTEGER,
    home_goals_ht   INTEGER,
    away_goals_ht   INTEGER,
    result          TEXT,
    duration        TEXT,
    referee         TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (match_id, league)
);
CREATE INDEX idx_matches_league_season ON matches (league, season);
CREATE INDEX idx_matches_date ON matches (date);
CREATE INDEX idx_matches_teams ON matches (home_team, away_team);

-- ── 2. SOFASCORE_MATCHES ─────────────────────────────────────
CREATE TABLE sofascore_matches (
    id              BIGSERIAL PRIMARY KEY,
    match_id        TEXT        NOT NULL,
    league          TEXT        NOT NULL,
    season          TEXT        NOT NULL,
    date            DATE,
    home_team       TEXT,
    away_team       TEXT,
    home_goals      NUMERIC,
    away_goals      NUMERIC,
    result          TEXT,
    home_ball_possession                NUMERIC,
    away_ball_possession                NUMERIC,
    home_expected_goals                 NUMERIC,
    away_expected_goals                 NUMERIC,
    home_big_chance_created             NUMERIC,
    away_big_chance_created             NUMERIC,
    home_big_chance_missed              NUMERIC,
    away_big_chance_missed              NUMERIC,
    home_total_shots_on_goal            NUMERIC,
    away_total_shots_on_goal            NUMERIC,
    home_shots_on_target                NUMERIC,
    away_shots_on_target                NUMERIC,
    home_shots_off_target               NUMERIC,
    away_shots_off_target               NUMERIC,
    home_blocked_shots                  NUMERIC,
    away_blocked_shots                  NUMERIC,
    home_goalkeeper_saves               NUMERIC,
    away_goalkeeper_saves               NUMERIC,
    home_corner_kicks                   NUMERIC,
    away_corner_kicks                   NUMERIC,
    home_yellow_cards                   NUMERIC,
    away_yellow_cards                   NUMERIC,
    home_red_cards                      NUMERIC,
    away_red_cards                      NUMERIC,
    home_total_passes                   NUMERIC,
    away_total_passes                   NUMERIC,
    home_accurate_passes                NUMERIC,
    away_accurate_passes                NUMERIC,
    home_accurate_passes_percentage     NUMERIC,
    away_accurate_passes_percentage     NUMERIC,
    home_fouls                          NUMERIC,
    away_fouls                          NUMERIC,
    home_offsides                       NUMERIC,
    away_offsides                       NUMERIC,
    home_dribbles                       NUMERIC,
    away_dribbles                       NUMERIC,
    home_successful_dribbles            NUMERIC,
    away_successful_dribbles            NUMERIC,
    home_tackles                        NUMERIC,
    away_tackles                        NUMERIC,
    home_total_duels                    NUMERIC,
    away_total_duels                    NUMERIC,
    home_total_duels_won                NUMERIC,
    away_total_duels_won                NUMERIC,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (match_id, league)
);
CREATE INDEX idx_ss_matches_league_season ON sofascore_matches (league, season);
CREATE INDEX idx_ss_matches_date ON sofascore_matches (date);

-- ── 3. TEAM_STATS ─────────────────────────────────────────────
CREATE TABLE team_stats (
    id                  BIGSERIAL PRIMARY KEY,
    league              TEXT    NOT NULL,
    season              TEXT    NOT NULL,
    team                TEXT    NOT NULL,
    played              INTEGER,
    wins                INTEGER,
    draws               INTEGER,
    losses              INTEGER,
    points              INTEGER,
    goals_scored        INTEGER,
    goals_conceded      INTEGER,
    goal_difference     INTEGER,
    home_wins           INTEGER,
    home_draws          INTEGER,
    home_losses         INTEGER,
    away_wins           INTEGER,
    away_draws          INTEGER,
    away_losses         INTEGER,
    clean_sheets        INTEGER,
    avg_goals_scored    NUMERIC,
    avg_goals_conceded  NUMERIC,
    home_win_rate       NUMERIC,
    away_win_rate       NUMERIC,
    form_last5          TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (league, season, team)
);
CREATE INDEX idx_team_stats_league_season ON team_stats (league, season);

-- ── 4. TEAM_STATS_ADVANCED ────────────────────────────────────
CREATE TABLE team_stats_advanced (
    id                                  BIGSERIAL PRIMARY KEY,
    league                              TEXT    NOT NULL,
    season                              TEXT    NOT NULL,
    team                                TEXT    NOT NULL,
    played                              INTEGER,
    wins                                INTEGER,
    draws                               INTEGER,
    losses                              INTEGER,
    points                              INTEGER,
    goals_scored                        INTEGER,
    goals_conceded                      INTEGER,
    goal_difference                     INTEGER,
    home_played                         INTEGER,
    home_wins                           INTEGER,
    away_played                         INTEGER,
    away_wins                           INTEGER,
    home_win_rate                       NUMERIC,
    away_win_rate                       NUMERIC,
    avg_ball_possession                 NUMERIC,
    home_avg_ball_possession            NUMERIC,
    away_avg_ball_possession            NUMERIC,
    avg_expected_goals                  NUMERIC,
    home_avg_expected_goals             NUMERIC,
    away_avg_expected_goals             NUMERIC,
    avg_big_chance_created              NUMERIC,
    home_avg_big_chance_created         NUMERIC,
    away_avg_big_chance_created         NUMERIC,
    avg_big_chance_missed               NUMERIC,
    home_avg_big_chance_missed          NUMERIC,
    away_avg_big_chance_missed          NUMERIC,
    avg_total_shots_on_goal             NUMERIC,
    home_avg_total_shots_on_goal        NUMERIC,
    away_avg_total_shots_on_goal        NUMERIC,
    avg_shots_on_target                 NUMERIC,
    home_avg_shots_on_target            NUMERIC,
    away_avg_shots_on_target            NUMERIC,
    avg_shots_off_target                NUMERIC,
    home_avg_shots_off_target           NUMERIC,
    away_avg_shots_off_target           NUMERIC,
    avg_blocked_shots                   NUMERIC,
    home_avg_blocked_shots              NUMERIC,
    away_avg_blocked_shots              NUMERIC,
    avg_goalkeeper_saves                NUMERIC,
    home_avg_goalkeeper_saves           NUMERIC,
    away_avg_goalkeeper_saves           NUMERIC,
    avg_corner_kicks                    NUMERIC,
    home_avg_corner_kicks               NUMERIC,
    away_avg_corner_kicks               NUMERIC,
    avg_yellow_cards                    NUMERIC,
    home_avg_yellow_cards               NUMERIC,
    away_avg_yellow_cards               NUMERIC,
    avg_red_cards                       NUMERIC,
    home_avg_red_cards                  NUMERIC,
    away_avg_red_cards                  NUMERIC,
    avg_total_passes                    NUMERIC,
    home_avg_total_passes               NUMERIC,
    away_avg_total_passes               NUMERIC,
    avg_accurate_passes                 NUMERIC,
    home_avg_accurate_passes            NUMERIC,
    away_avg_accurate_passes            NUMERIC,
    avg_accurate_passes_percentage      NUMERIC,
    home_avg_accurate_passes_percentage NUMERIC,
    away_avg_accurate_passes_percentage NUMERIC,
    avg_fouls                           NUMERIC,
    home_avg_fouls                      NUMERIC,
    away_avg_fouls                      NUMERIC,
    avg_offsides                        NUMERIC,
    home_avg_offsides                   NUMERIC,
    away_avg_offsides                   NUMERIC,
    avg_dribbles                        NUMERIC,
    home_avg_dribbles                   NUMERIC,
    away_avg_dribbles                   NUMERIC,
    avg_successful_dribbles             NUMERIC,
    home_avg_successful_dribbles        NUMERIC,
    away_avg_successful_dribbles        NUMERIC,
    avg_tackles                         NUMERIC,
    home_avg_tackles                    NUMERIC,
    away_avg_tackles                    NUMERIC,
    avg_total_duels                     NUMERIC,
    home_avg_total_duels                NUMERIC,
    away_avg_total_duels                NUMERIC,
    avg_total_duels_won                 NUMERIC,
    home_avg_total_duels_won            NUMERIC,
    away_avg_total_duels_won            NUMERIC,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (league, season, team)
);
CREATE INDEX idx_adv_stats_league_season ON team_stats_advanced (league, season);

-- ── RLS ───────────────────────────────────────────────────────
ALTER TABLE matches              ENABLE ROW LEVEL SECURITY;
ALTER TABLE sofascore_matches    ENABLE ROW LEVEL SECURITY;
ALTER TABLE team_stats           ENABLE ROW LEVEL SECURITY;
ALTER TABLE team_stats_advanced  ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Public read" ON matches             FOR SELECT USING (true);
CREATE POLICY "Public read" ON sofascore_matches   FOR SELECT USING (true);
CREATE POLICY "Public read" ON team_stats          FOR SELECT USING (true);
CREATE POLICY "Public read" ON team_stats_advanced FOR SELECT USING (true);