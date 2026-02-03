select
    session_key as session_id,
    meeting_key as meeting_id,
    driver_number,
    date_start as time_start,
    lap_number,
    lap_duration,
    duration_sector_1,
    duration_sector_2,
    duration_sector_3,
    i1_speed,
    i2_speed,
    st_speed,
    is_pit_out_lap,
    time_loaded
from {{ source("openf1_api", "laps")}}

