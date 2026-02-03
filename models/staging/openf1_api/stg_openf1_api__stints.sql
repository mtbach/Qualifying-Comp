select
    session_key as session_id,
    meeting_key as meeting_id,
    driver_number,
    stint_number,
    lap_start,
    lap_end,
    compound,
    tyre_age_at_start,
    time_loaded
from {{ source("openf1_api", "stint")}}