select 
    date as time,
    session_key as session_id,
    meeting_key as meeting_id,
    driver_number,
    x,
    y,
    z,
    time_loaded
from {{ source("openf1_api", "car_location")}}