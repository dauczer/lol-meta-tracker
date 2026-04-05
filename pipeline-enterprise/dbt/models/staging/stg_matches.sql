-- Staging model: flatten raw match JSON into participant-level rows.
--
-- In the enterprise stack, raw match JSON would be loaded into a BigQuery /
-- Snowflake table (e.g. via a Cloud Function or Fivetran connector) before
-- this model runs.  The portfolio equivalent is pipeline/transform.py's
-- parse_matches() function.
--
-- Source table schema (BigQuery example):
--   raw_matches.match_id    STRING
--   raw_matches.info        JSON   (the full "info" object from Match V5)
--   raw_matches.loaded_at   TIMESTAMP

{{
  config(
    materialized='view',
    description='One row per participant per match, parsed from raw JSON'
  )
}}

WITH source AS (
    SELECT
        match_id,
        info,
        loaded_at
    FROM {{ source('riot_raw', 'raw_matches') }}
),

participants AS (
    SELECT
        match_id,
        loaded_at,
        -- BigQuery JSON unnesting syntax
        JSON_VALUE(p, '$.championName')    AS champion_name,
        JSON_VALUE(p, '$.teamPosition')    AS team_position,
        CAST(JSON_VALUE(p, '$.win') AS BOOL)   AS win,
        CAST(JSON_VALUE(p, '$.kills') AS INT64) AS kills,
        CAST(JSON_VALUE(p, '$.deaths') AS INT64) AS deaths,
        CAST(JSON_VALUE(p, '$.assists') AS INT64) AS assists,
        CAST(JSON_VALUE(info, '$.gameDuration') AS INT64) AS game_duration,
        -- Extract "14.7" from "14.7.596.9818"
        CONCAT(
            SPLIT(JSON_VALUE(info, '$.gameVersion'), '.')[OFFSET(0)],
            '.',
            SPLIT(JSON_VALUE(info, '$.gameVersion'), '.')[OFFSET(1)]
        ) AS patch
    FROM source,
    UNNEST(JSON_QUERY_ARRAY(info, '$.participants')) AS p
)

SELECT *
FROM participants
WHERE
    game_duration >= 900          -- exclude remakes
    AND team_position != ''       -- exclude missing role assignments
