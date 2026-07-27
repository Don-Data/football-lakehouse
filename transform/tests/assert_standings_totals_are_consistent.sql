select *
from {{ ref('fct_team_standings') }}
where played != won + drawn + lost
   or points != (won * 3 + drawn)
