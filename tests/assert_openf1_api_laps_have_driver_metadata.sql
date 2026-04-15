with drivers as (
    select distinct
        session_id,
        driver_number
    from {{ ref('stg_openf1_api__drivers') }}
),

lap_drivers as (
    select distinct
        session_id,
        driver_number
    from {{ ref('stg_openf1_api__laps') }}
)

select
    lap_drivers.session_id,
    lap_drivers.driver_number
from lap_drivers
left join drivers
    on lap_drivers.session_id = drivers.session_id
    and lap_drivers.driver_number = drivers.driver_number
where drivers.driver_number is null
