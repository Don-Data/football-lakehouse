select
    id as match_id,
    utc_date as match_utc_date,
    status,
    matchday,
    stage,

    home_team__id as home_team_id,
    home_team__name as home_team_name,
    home_team__short_name as home_team_short_name,

    away_team__id as away_team_id,
    away_team__name as away_team_name,
    away_team__short_name as away_team_short_name,

    competition__id as competition_id,
    competition__name as competition_name,

    season__id as season_id,
    season__start_date as season_start_date,
    season__end_date as season_end_date,

    score__winner as score_winner,
    score__full_time__home as full_time_home_goals,
    score__full_time__away as full_time_away_goals,
    score__half_time__home as half_time_home_goals,
    score__half_time__away as half_time_away_goals

from {{ source('bronze', 'matches') }}
