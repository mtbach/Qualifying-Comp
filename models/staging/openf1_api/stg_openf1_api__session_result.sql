select
    session_key as session_id,
    meeting_key as meeting_id,
    driver_number,
    position,
    number_of_laps,
    duration_q1,
    duration_q2,
    duration_q3,
    dnf as is_dnf,
    dns as is_dns,
    dsq as is_dsq,
    time_loaded
from {{ source("openf1_api", "session_result")}}