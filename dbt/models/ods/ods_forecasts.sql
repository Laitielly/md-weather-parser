{{ config(
    materialized='incremental',
    unique_key='forecast_id',
    incremental_strategy='delete+insert',
    tags=['ods', 'silver']
) }}

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
    is_valid_forecast,
    -- Вычисление горизонта прогноза
    DATE_PART('hour', forecast_ts - collected_at) as forecast_horizon_hours,
    CASE 
        WHEN DATE_PART('hour', forecast_ts - collected_at) <= 12 THEN 'short_term'
        WHEN DATE_PART('hour', forecast_ts - collected_at) <= 48 THEN 'medium_term'
        ELSE 'long_term'
    END as forecast_type,
    -- Категории прогноза
    CASE 
        WHEN forecasted_temperature <= 0 THEN 'freezing'
        WHEN forecasted_temperature <= 10 THEN 'cold'
        WHEN forecasted_temperature <= 20 THEN 'cool'
        WHEN forecasted_temperature <= 30 THEN 'warm'
        ELSE 'hot'
    END as forecasted_temperature_category
FROM {{ ref('stg_forecasts') }}
WHERE is_valid_forecast = true

{% if is_incremental() %}
    AND collected_at > (SELECT MAX(collected_at) FROM {{ this }})
{% endif %}