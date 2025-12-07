{{ config(
    materialized='table',
    tags=['dwh', 'gold']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['weather_main', 'temperature_category']) }} as weather_type_id,
    weather_main,
    temperature_category,
    COUNT(*) as measurement_count
FROM {{ ref('ods_current_weather') }}
GROUP BY 1, 2, 3