
select 
    {{ dbt_utils.generate_surrogate_key(['session_key', 'time_loaded'])}} as session_sk,
    session_key as session_id,
    upper(session_type) as session_type,
    upper(session_name) as session_name,
    date_start,
    date_end,
    meeting_key as meeting_id,
    circuit_key as circuit_id,
    upper(circuit_short_name) as circuit_name,
    country_key as country_id,
    country_code,
    upper(location) as city,
    year,
    time_loaded
from {{ source('openf1_api', 'sessions')}}