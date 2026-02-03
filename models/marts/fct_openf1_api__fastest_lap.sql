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
)
select * from sorted
where rn = 1 
order by time_start
