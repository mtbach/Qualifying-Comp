use warehouse compute_wh;
with 
laps as (
    select *
    from {{ ref("stg_openf1_api__laps") }} ),

sessions as (
    select
        session_id,
        session_type
    from {{ ref("stg_openf1_api__sessions")}}
    where session_type  like '%QUALIFYING%' or session_type like '%PRACTICE%'
),
tires as (
    select session_id,
        driver_number,
        compound,
        tyre_age_at_start,
        lap_start,
        lap_end
    from {{ ref("stg_openf1_api__stints")}}
),
quali_prac as (
    select 
    laps.session_id,
    session_type,
    driver_number,
    lap_number,
    lap_duration,
    duration_sector_1,
    duration_sector_2,
    duration_sector_3,
    time_start
    from
    laps join sessions on laps.session_id=sessions.session_id 
),
sorted as (
    select *,
    row_number() over (
        partition by driver_number, session_id
        order by lap_duration asc
    ) as rn
    from quali_prac
),
final as (
    select 
        sorted.session_id,
        session_type,
        sorted.driver_number,
        lap_number,
        lap_duration,
        duration_sector_1,
        duration_sector_2,
        duration_sector_3,
        compound,
        (tyre_age_at_start + lap_number - lap_start) as tyre_age,
        rn,
        time_start
    from sorted join tires on sorted.session_id = tires.session_id and sorted.driver_number=tires.driver_number
    where lap_number > lap_start and lap_number < lap_end
    and rn = 1
)
select * from final

order by time_start
