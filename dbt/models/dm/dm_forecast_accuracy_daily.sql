{{ config(
    materialized='incremental',
    unique_key='accuracy_date',
    incremental_strategy='append',
    tags=['dm', 'mart']
) }}

SELECT
    DATE(verification_dt) as accuracy_date,
    forecast_horizon_hours,
    COUNT(*) as forecasts_count,
    AVG(temp_absolute_error) as avg_temperature_error,
    STDDEV(temp_error) as std_temperature_error,
    AVG(ABS(humidity_error)) as avg_humidity_error,
    AVG(ABS(pressure_error)) as avg_pressure_error,
    SUM(CASE WHEN weather_match THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100 as weather_match_percent,
    -- Категоризация точности
    CASE 
        WHEN AVG(temp_absolute_error) <= 1 THEN 'high'
        WHEN AVG(temp_absolute_error) <= 3 THEN 'medium'
        ELSE 'low'
    END as accuracy_category
FROM {{ source('analytics', 'accuracy_metrics') }} am
LEFT JOIN {{ ref('ods_forecasts') }} of 
    ON DATE_TRUNC('hour', TO_TIMESTAMP(am.forecast_dt)) = DATE_TRUNC('hour', of.forecast_ts)
WHERE verification_dt IS NOT NULL
GROUP BY 1, 2

{% if is_incremental() %}
    HAVING DATE(verification_dt) > (SELECT MAX(accuracy_date) FROM {{ this }})
{% endif %}