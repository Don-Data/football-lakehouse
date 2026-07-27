with team_matches as (
    select * from {{ ref('int_team_match_results') }}
),

aggregated as (
    select
        competition_id,
        competition_name,
        team_id,
        team_name,
        count(*) as played,
        sum(case when points_earned = 3 then 1 else 0 end) as won,
        sum(case when points_earned = 1 then 1 else 0 end) as drawn,
        sum(case when points_earned = 0 then 1 else 0 end) as lost,
        sum(goals_for) as goals_for,
        sum(goals_against) as goals_against,
        sum(goals_for) - sum(goals_against) as goal_difference,
        sum(points_earned) as points
    from team_matches
    group by competition_id, competition_name, team_id, team_name
)

select
    *,
    row_number() over (
        partition by competition_id
        order by points desc, goal_difference desc, goals_for desc
    ) as position
from aggregated
order by position
