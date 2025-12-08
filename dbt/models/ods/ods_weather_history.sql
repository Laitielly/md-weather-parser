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
    collected_ts as loaded_at
from {{ ref('stg_current_weather') }}

{% if is_incremental() %}
    where collected_ts > (select max(loaded_at) from {{ this }})
{% endif %}