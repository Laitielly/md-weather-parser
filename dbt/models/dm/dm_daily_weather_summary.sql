{{ config(
    materialized='incremental',
    unique_key='summary_date',
    incremental_strategy='merge',
    tags=['dm', 'mart']
) }}

WITH daily_weather AS (
    SELECT
        DATE(measurement_ts) as summary_date,
        COUNT(*) as measurements_count,
        AVG(temperature_c) as avg_temperature,
        MIN(temperature_c) as min_temperature,
        MAX(temperature_c) as max_temperature,
        AVG(humidity_percent) as avg_humidity,
        AVG(pressure_hpa) as avg_pressure,
        AVG(wind_speed_ms) as avg_wind_speed,
        MODE() WITHIN GROUP (ORDER BY weather_main) as most_common_weather,
        SUM(CASE WHEN precipitation_probability > 0.5 THEN 1 ELSE 0 END) as high_precipitation_hours
    FROM {{ ref('fact_weather_measurements') }} f
    LEFT JOIN {{ ref('ods_forecasts') }} of ON f.weather_type_id = {{ dbt_utils.surrogate_key(['of.forecasted_weather_main', 'of.forecasted_temperature_category']) }}
    GROUP BY 1
),

daily_forecast_accuracy AS (
    SELECT
        DATE(verification_dt) as accuracy_date,
        AVG(temp_absolute_error) as avg_temp_error,
        AVG(CAST(weather_match AS INT)) * 100 as weather_match_percent
    FROM {{ source('analytics', 'accuracy_metrics') }}
    GROUP BY 1
)

SELECT
    dw.summary_date,
    dw.measurements_count,
    dw.avg_temperature,
    dw.min_temperature,
    dw.max_temperature,
    dw.avg_humidity,
    dw.avg_pressure,
    dw.avg_wind_speed,
    dw.most_common_weather,
    dw.high_precipitation_hours,
    COALESCE(dfa.avg_temp_error, 0) as avg_forecast_error,
    COALESCE(dfa.weather_match_percent, 0) as weather_forecast_accuracy
FROM daily_weather dw
LEFT JOIN daily_forecast_accuracy dfa ON dw.summary_date = dfa.accuracy_date

{% if is_incremental() %}
    WHERE dw.summary_date > (SELECT MAX(summary_date) FROM {{ this }})
{% endif %}