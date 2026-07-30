with matches as (
    select * from {{ ref('int_matches_enriched') }}
),

home_perspective as (
    select
        match_id,
        match_utc_date,
        matchday,
        competition_id,
        competition_name,
        season_id,

        home_team_id as team_id,
        home_team_name as team_name,
        away_team_id as opponent_id,
        away_team_name as opponent_name,
        true as is_home,

        full_time_home_goals as goals_for,
        full_time_away_goals as goals_against,

        case match_result
            when 'HOME_TEAM' then 3
            when 'DRAW' then 1
            when 'AWAY_TEAM' then 0
        end as points_earned

    from matches
    where status = 'FINISHED'
),

away_perspective as (
    select
        match_id,
        match_utc_date,
        matchday,
        competition_id,
        competition_name,
        season_id,

        away_team_id as team_id,
        away_team_name as team_name,
        home_team_id as opponent_id,
        home_team_name as opponent_name,
        false as is_home,

        full_time_away_goals as goals_for,
        full_time_home_goals as goals_against,

        case match_result
            when 'AWAY_TEAM' then 3
            when 'DRAW' then 1
            when 'HOME_TEAM' then 0
        end as points_earned

    from matches
    where status = 'FINISHED'
)

select * from home_perspective
union all
select * from away_perspective
