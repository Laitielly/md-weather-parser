-- Кастомный тест для проверки положительной температуры (в градусах Цельсия)
SELECT 
    weather_id,
    measurement_ts,
    temperature_c
FROM {{ ref('stg_current_weather') }}
WHERE temperature_c < -273.15  -- Абсолютный ноль
   OR temperature_c > 100       -- Нереально высокая температура