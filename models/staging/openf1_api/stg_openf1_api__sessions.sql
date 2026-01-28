
select 
    session_key as session_id,
    session_type,
    session_name,
    date_start,
    date_end,
    meeting_key as meeting_id,
    circuit_key as circuit_id,
    circuit_short_name as circuit_name,
    country_key as country_id,
    country_code,
    location as city,
    year
from {{ source('openf1_api', 'sessions')}}