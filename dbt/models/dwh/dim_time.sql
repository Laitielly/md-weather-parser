{{ config(
    materialized='table',
    tags=['dwh', 'gold']
) }}

WITH time_series AS (
    SELECT 
        DISTINCT 
        DATE_TRUNC('hour', measurement_ts) as hour_ts
    FROM {{ ref('ods_current_weather') }}
    
    UNION
    
    SELECT 
        DISTINCT 
        DATE_TRUNC('hour', forecast_ts) as hour_ts
    FROM {{ ref('ods_forecasts') }}
    
    UNION
    
    SELECT 
        DISTINCT 
        DATE_TRUNC('hour', collected_at) as hour_ts
    FROM {{ ref('ods_current_weather') }}
),

enriched AS (
    SELECT
        hour_ts,
        EXTRACT(YEAR FROM hour_ts) as year,
        EXTRACT(MONTH FROM hour_ts) as month,
        EXTRACT(DAY FROM hour_ts) as day,
        EXTRACT(HOUR FROM hour_ts) as hour,
        EXTRACT(DOW FROM hour_ts) as day_of_week,
        EXTRACT(DOY FROM hour_ts) as day_of_year,
        CASE 
            WHEN EXTRACT(MONTH FROM hour_ts) IN (12, 1, 2) THEN 'winter'
            WHEN EXTRACT(MONTH FROM hour_ts) IN (3, 4, 5) THEN 'spring'
            WHEN EXTRACT(MONTH FROM hour_ts) IN (6, 7, 8) THEN 'summer'
            ELSE 'autumn'
        END as season,
        CASE 
            WHEN EXTRACT(HOUR FROM hour_ts) BETWEEN 6 AND 18 THEN 'day'
            ELSE 'night'
        END as time_of_day
    FROM time_series
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['hour_ts']) }} as time_key,
    *
FROM enriched