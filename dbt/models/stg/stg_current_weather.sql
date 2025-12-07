{{ config(
    materialized='view',
    tags=['stg', 'bronze']
) }}

WITH source AS (
    SELECT
        id as weather_id,
        city,
        country,
        dt as unix_timestamp,
        TO_TIMESTAMP(dt) as measurement_ts,
        temp as temperature_c,
        feels_like as feels_like_c,
        pressure as pressure_hpa,
        humidity as humidity_percent,
        wind_speed as wind_speed_ms,
        wind_deg as wind_direction_deg,
        weather_main,
        weather_description,
        clouds as cloudiness_percent,
        visibility as visibility_meters,
        collected_ts as collected_at,
        created_at as loaded_at
    FROM {{ source('analytics', 'current_weather') }}
    WHERE dt IS NOT NULL
      AND temp IS NOT NULL
      AND collected_ts IS NOT NULL
)

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
    -- Проверка качества данных
    CASE 
        WHEN temperature_c < -50 OR temperature_c > 50 THEN false
        WHEN humidity_percent < 0 OR humidity_percent > 100 THEN false
        WHEN pressure_hpa < 870 OR pressure_hpa > 1085 THEN false
        ELSE true
    END as is_valid_measurement
FROM source