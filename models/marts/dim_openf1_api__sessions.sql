use warehouse compute_wh;
select 
    session_sk,
    session_id,
    session_type,
    session_name,
    date_start,
    date_end,
    meeting_id,
    circuit_id,
    circuit_name,
    country_code,
    city,
    year
from {{ ref("stg_openf1_api__sessions")}}