{{ config(
    materialized='view',
    tags=['stg', 'bronze']
) }}

WITH source AS (
    SELECT
        id as forecast_id,
        city,
        country,
        forecast_dt as forecast_unix_timestamp,
        TO_TIMESTAMP(forecast_dt) as forecast_ts,
        collection_dt as collected_at,
        temp as forecasted_temperature,
        feels_like as forecasted_feels_like,
        pressure as forecasted_pressure,
        humidity as forecasted_humidity,
        wind_speed as forecasted_wind_speed,
        wind_deg as forecasted_wind_direction,
        weather_main as forecasted_weather_main,
        weather_description as forecasted_weather_desc,
        clouds as forecasted_cloudiness,
        pop as precipitation_probability,
        created_at as loaded_at
    FROM {{ source('analytics', 'forecasts') }}
    WHERE forecast_dt IS NOT NULL
      AND collection_dt IS NOT NULL
      AND temp IS NOT NULL
)

SELECT
    forecast_id,
    city,
    country,
    forecast_unix_timestamp,
    forecast_ts,
    collected_at,
    forecasted_temperature,
    forecasted_feels_like,
    forecasted_pressure,
    forecasted_humidity,
    forecasted_wind_speed,
    forecasted_wind_direction,
    forecasted_weather_main,
    forecasted_weather_desc,
    forecasted_cloudiness,
    precipitation_probability,
    loaded_at,
    -- Проверка качества данных
    CASE 
        WHEN forecasted_temperature < -50 OR forecasted_temperature > 50 THEN false
        WHEN forecasted_humidity < 0 OR forecasted_humidity > 100 THEN false
        WHEN forecasted_pressure < 870 OR forecasted_pressure > 1085 THEN false
        ELSE true
    END as is_valid_forecast
FROM source