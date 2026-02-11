select 
    session_key as session_id,
    meeting_key as meeting_id,
    driver_number,
    position,
    duration,
    number_of_laps,
    dnf as is_dnf,
    dns as is_dns,
    dsq as is_dsq
from {{ source("openf1_api", "fp3_result")}}