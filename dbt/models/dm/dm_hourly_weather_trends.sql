{{ config(
    materialized='table',
    tags=['dm', 'mart']
) }}

WITH hourly_data AS (
    SELECT
        DATE_TRUNC('hour', measurement_ts) as hour_start,
        EXTRACT(HOUR FROM measurement_ts) as hour_of_day,
        AVG(temperature_c) as avg_temperature,
        AVG(humidity_percent) as avg_humidity,
        AVG(wind_speed_ms) as avg_wind_speed,
        COUNT(*) as measurements_count,
        MODE() WITHIN GROUP (ORDER BY weather_main) as most_common_weather
    FROM {{ ref('fact_weather_measurements') }}
    GROUP BY 1, 2
),

hourly_forecast_accuracy AS (
    SELECT
        DATE_TRUNC('hour', verification_dt) as hour_start,
        AVG(temp_absolute_error) as avg_forecast_error
    FROM {{ source('analytics', 'accuracy_metrics') }}
    GROUP BY 1
)

SELECT
    hd.hour_start,
    hd.hour_of_day,
    hd.avg_temperature,
    hd.avg_humidity,
    hd.avg_wind_speed,
    hd.measurements_count,
    hd.most_common_weather,
    COALESCE(hfa.avg_forecast_error, 0) as avg_forecast_error,
    -- Тренд температуры
    AVG(hd.avg_temperature) OVER (
        PARTITION BY hd.hour_of_day 
        ORDER BY hd.hour_start 
        ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
    ) as prev_24h_avg_temperature,
    hd.avg_temperature - AVG(hd.avg_temperature) OVER (
        PARTITION BY hd.hour_of_day 
        ORDER BY hd.hour_start 
        ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
    ) as temperature_change_from_avg
FROM hourly_data hd
LEFT JOIN hourly_forecast_accuracy hfa ON hd.hour_start = hfa.hour_start
ORDER BY hd.hour_start DESC