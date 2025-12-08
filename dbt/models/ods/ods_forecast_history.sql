{{
    config(
        materialized='incremental',
        unique_key=['city', 'forecast_timestamp', 'mongo_collection_ts'],
        strategy='merge',
        tags=["ods"]
    )
}}

select
    raw_id,
    city,
    country,
    forecast_timestamp,
    mongo_collection_ts,
    temp,
    humidity,
    forecast_weather_main,
    pg_loaded_at as loaded_at 
from {{ ref('stg_forecasts') }}

{% if is_incremental() %}
    where pg_loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}