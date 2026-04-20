select  
    {{ dbt_utils.generate_surrogate_key(['session_key', 'driver_number', 'time_loaded'])}} as driver_sk,
    session_key as session_id,
    meeting_key as meeting_id,
    upper(full_name) as full_name,
    upper(first_name) as first_name,
    upper(last_name) as last_name,
    driver_number,
    name_acronym,
    upper(broadcast_name) as broadcast_name,
    upper(team_name) as team_name,
    time_loaded

from {{ source("openf1_api", "drivers")}}