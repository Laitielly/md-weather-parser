{{
    config(
        materialized='incremental',
        unique_key='stat_date',
        incremental_strategy='delete+insert',
        tags=["dm"]
    )
}}

with source_data as (
    select * from {{ ref('ods_weather_history') }}
)

select
    date_trunc('day', observation_dt)::date as stat_date,
    city,
    avg(temp) as avg_temp,
    min(temp) as min_temp,
    max(temp) as max_temp,
    count(*) as records_count,
    now() as dbt_updated_at
from source_data

{% if is_incremental() %}
    where observation_dt >= current_date - interval '3 days'
{% endif %}

group by 1, 2