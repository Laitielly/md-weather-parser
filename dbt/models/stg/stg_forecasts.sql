{{ config(tags=["stg"]) }}

with source as (
    select * from {{ source('weather_raw', 'forecasts') }}
)

select
    id as raw_id,
    to_timestamp(forecast_dt) as forecast_dt,
    collection_dt,
    cast(temp as numeric) as forecast_temp,
    weather_main as forecast_weather
from source