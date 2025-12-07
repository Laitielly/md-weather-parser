{{ config(
    materialized='table',
    tags=['dwh', 'gold']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['w.weather_id', 'w.measurement_ts']) }} as measurement_id,
    {{ dbt_utils.generate_surrogate_key(['DATE_TRUNC(\'hour\', w.measurement_ts)']) }} as time_key,
    {{ dbt_utils.generate_surrogate_key(['w.weather_main', 'w.temperature_category']) }} as weather_type_id,
    w.weather_id,
    w.measurement_ts,
    w.temperature_c,
    w.feels_like_c,
    w.pressure_hpa,
    w.humidity_percent,
    w.wind_speed_ms,
    w.wind_direction_deg,
    w.cloudiness_percent,
    w.visibility_meters,
    w.collected_at,
    -- Найти соответствующий прогноз (ближайший по времени)
    f.forecasted_temperature,
    f.forecasted_temperature - w.temperature_c as forecast_error_temperature,
    ABS(f.forecasted_temperature - w.temperature_c) as forecast_absolute_error_temperature,
    -- Вычислить точность прогноза
    CASE 
        WHEN ABS(f.forecasted_temperature - w.temperature_c) <= 1 THEN 'high'
        WHEN ABS(f.forecasted_temperature - w.temperature_c) <= 3 THEN 'medium'
        ELSE 'low'
    END as forecast_accuracy
FROM {{ ref('ods_current_weather') }} w
LEFT JOIN {{ ref('ods_forecasts') }} f 
    ON DATE_TRUNC('hour', w.measurement_ts) = DATE_TRUNC('hour', f.forecast_ts)
    AND f.forecast_horizon_hours <= 24
    AND f.collected_at <= w.measurement_ts
WHERE w.is_valid_measurement = true