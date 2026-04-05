-- Mart model: champion-level statistics per role per patch.
--
-- This is the SQL equivalent of pipeline/transform.py's aggregate_stats()
-- and top_champions_per_role() functions.
--
-- Consumers: BI dashboards, the portfolio website API, downstream models.

{{
  config(
    materialized='table',
    description='Aggregated win rate, pick rate, and KDA per champion, role, and patch'
  )
}}

WITH base AS (
    SELECT * FROM {{ ref('stg_matches') }}
),

patch_match_counts AS (
    -- Number of unique matches per patch (for pick_rate denominator)
    SELECT
        patch,
        COUNT(DISTINCT match_id) AS total_matches
    FROM base
    GROUP BY patch
),

aggregated AS (
    SELECT
        b.champion_name,
        b.team_position,
        b.patch,
        COUNT(*)                                    AS games_played,
        SUM(CASE WHEN b.win THEN 1 ELSE 0 END)     AS wins,
        SUM(b.kills)                                AS total_kills,
        SUM(b.deaths)                               AS total_deaths,
        SUM(b.assists)                              AS total_assists,
        p.total_matches
    FROM base b
    JOIN patch_match_counts p USING (patch)
    GROUP BY
        b.champion_name,
        b.team_position,
        b.patch,
        p.total_matches
)

SELECT
    champion_name,
    team_position,
    patch,
    games_played,
    wins,
    ROUND(wins / games_played, 4)                               AS win_rate,
    -- 2 teams per match → each match has 2 picks per role
    ROUND(games_played / (total_matches * 2), 4)                AS pick_rate,
    ROUND(
        (total_kills + total_assists) / GREATEST(total_deaths, 1),
        2
    )                                                            AS avg_kda
FROM aggregated
WHERE games_played >= 20   -- minimum threshold for statistical relevance
ORDER BY patch DESC, win_rate DESC
