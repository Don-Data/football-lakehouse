select
    id as competition_id,
    name as competition_name,
    area__name as area_name,
    plan
from {{ source('bronze', 'competitions') }}
