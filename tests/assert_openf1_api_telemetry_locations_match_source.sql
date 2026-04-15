with telemetry_facts as (
    select
        session_id,
        driver_number,
        time,
        x,
        y
    from {{ ref('fct_openf1_api__telemetry_4') }}

    union all

    select
        session_id,
        driver_number,
        time,
        x,
        y
    from {{ ref('fct_openf1_api__telemetry_81') }}
),

source_locations as (
    select
        session_id,
        driver_number,
        time,
        x,
        y
    from {{ ref('stg_openf1_api__car_location') }}
)

select
    telemetry_facts.session_id,
    telemetry_facts.driver_number,
    telemetry_facts.time,
    telemetry_facts.x,
    telemetry_facts.y
from telemetry_facts
left join source_locations
    on telemetry_facts.session_id = source_locations.session_id
    and telemetry_facts.driver_number = source_locations.driver_number
    and telemetry_facts.time = source_locations.time
    and telemetry_facts.x = source_locations.x
    and telemetry_facts.y = source_locations.y
where source_locations.time is null
