{{ config(tags=["stg"]) }}

with source as (
    select * from {{ source('weather_raw', 'current_weather') }}
)

select
    id as raw_id,
    city,
    to_timestamp(dt) as observation_dt,
    cast(temp as numeric) as temp,
    humidity,
    weather_main,
    collected_ts
from source