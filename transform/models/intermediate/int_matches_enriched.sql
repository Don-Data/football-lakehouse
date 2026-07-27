with matches as (
    select * from {{ ref('stg_matches') }}
),

competitions as (
    select * from {{ ref('stg_competitions') }}
)

select
    matches.match_id,
    matches.match_utc_date,
    matches.status,
    matches.matchday,

    matches.home_team_id,
    matches.home_team_name,
    matches.away_team_id,
    matches.away_team_name,

    matches.competition_id,
    competitions.competition_name,
    competitions.area_name,

    matches.full_time_home_goals,
    matches.full_time_away_goals,

    matches.score_winner as match_result

from matches
left join competitions
    on matches.competition_id = competitions.competition_id
