{{
    config(
        materialized='incremental',
        unique_key=['city', 'observation_dt'],
        strategy='merge',
        tags=["ods"]
    )
}}

select
    city,
    observation_dt,
    temp,
    humidity,
    weather_main,
    pg_loaded_at as loaded_at 
from {{ ref('stg_current_weather') }}

{% if is_incremental() %}
    where pg_loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}