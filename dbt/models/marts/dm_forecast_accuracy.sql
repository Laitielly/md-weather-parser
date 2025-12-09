{{
    config(
        materialized='incremental',
        unique_key='accuracy_date',
        incremental_strategy='append',
        tags=["dm", "accuracy"]
    )
}}

WITH actual_weather AS (
    SELECT 
        city,
        date_trunc('day', observation_dt)::date as weather_date,
        AVG(temp) as actual_avg_temp,
        MAX(temp) as actual_max_temp,
        MIN(temp) as actual_min_temp
    FROM {{ ref('ods_weather_history') }}
    WHERE observation_dt >= current_date - interval '30 days'
    GROUP BY 1, 2
),
forecast_data AS (
    SELECT 
        city,
        date_trunc('day', forecast_timestamp)::date as forecast_date,
        AVG(temp) as forecast_avg_temp,
        MAX(temp) as forecast_max_temp,
        MIN(temp) as forecast_min_temp,
        COUNT(*) as forecast_count
    FROM {{ ref('ods_forecast_history') }}
    WHERE forecast_timestamp >= current_date - interval '30 days'
    GROUP BY 1, 2
),
accuracy_calc AS (
    SELECT
        COALESCE(a.weather_date, f.forecast_date) as accuracy_date,
        COALESCE(a.city, f.city) as city,
        -- Расчет точности температур
        ABS(a.actual_avg_temp - f.forecast_avg_temp) as avg_temp_error,
        ABS(a.actual_max_temp - f.forecast_max_temp) as max_temp_error,
        ABS(a.actual_min_temp - f.forecast_min_temp) as min_temp_error,
        -- Процент ошибки
        CASE 
            WHEN a.actual_avg_temp != 0 
            THEN ABS(a.actual_avg_temp - f.forecast_avg_temp) / NULLIF(a.actual_avg_temp, 0) * 100 
            ELSE NULL 
        END as avg_temp_error_percent,
        a.actual_avg_temp,
        f.forecast_avg_temp,
        f.forecast_count,
        now() as calculated_at
    FROM actual_weather a
    FULL OUTER JOIN forecast_data f 
        ON a.city = f.city 
        AND a.weather_date = f.forecast_date
)

SELECT * FROM accuracy_calc
WHERE accuracy_date IS NOT NULL
{% if is_incremental() %}
    AND accuracy_date >= current_date - interval '7 days'
{% endif %}