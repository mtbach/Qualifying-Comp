use warehouse compute_wh;
with car as (
    select
    session_id,
    meeting_id,
    driver_number,
    time,
    speed,
    rpm,
    n_gear,
    throttle,
    brake,
    drs
    from {{ ref("stg_openf1_api__car_telemetry")}}
    where speed > 0 and driver_number = 81
),
location as (
    select
    time,
    x,
    y
    from {{ ref("stg_openf1_api__car_location")}}
    where driver_number = 81
),
final_telemetry as (
    select 
    session_id,
    meeting_id,
    driver_number,
    car.time as time,
    speed, 
    throttle,
    brake,
    rpm,
    n_gear,
    drs,
    x,
    y
    from car inner join location on car.time = location.time
)
select * from final_telemetry 