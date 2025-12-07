{{ config(
    materialized='incremental',
    unique_key='weather_id',
    incremental_strategy='merge',
    tags=['ods', 'silver']
) }}

SELECT
    weather_id,
    city,
    country,
    unix_timestamp,
    measurement_ts,
    temperature_c,
    feels_like_c,
    pressure_hpa,
    humidity_percent,
    wind_speed_ms,
    wind_direction_deg,
    weather_main,
    weather_description,
    cloudiness_percent,
    visibility_meters,
    collected_at,
    loaded_at,
    is_valid_measurement,
    -- Дополнительные вычисляемые поля
    CASE 
        WHEN temperature_c <= 0 THEN 'freezing'
        WHEN temperature_c <= 10 THEN 'cold'
        WHEN temperature_c <= 20 THEN 'cool'
        WHEN temperature_c <= 30 THEN 'warm'
        ELSE 'hot'
    END as temperature_category,
    CASE 
        WHEN humidity_percent < 30 THEN 'dry'
        WHEN humidity_percent < 60 THEN 'comfortable'
        WHEN humidity_percent < 80 THEN 'humid'
        ELSE 'very_humid'
    END as humidity_category,
    {{ dbt_utils.surrogate_key(['weather_main', 'temperature_category']) }} as weather_condition_id
FROM {{ ref('stg_current_weather') }}
WHERE is_valid_measurement = true

{% if is_incremental() %}
    AND collected_at > (SELECT MAX(collected_at) FROM {{ this }})
{% endif %}