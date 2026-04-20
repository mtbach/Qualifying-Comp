select
    session_key as session_id,
    meeting_key as meeting_id,
    driver_number,
    date as time,
    speed,
    rpm,
    n_gear,
    throttle,
    brake,
    drs,
    time_loaded
from {{ source("openf1_api", "car_telemetry")}}