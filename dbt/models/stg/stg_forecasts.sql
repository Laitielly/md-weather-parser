{{ config(tags=["stg"]) }}

with source as (
    select * from {{ source('weather_raw', 'forecasts') }}
)

select
    id as raw_id,
    (doc->>'city')::text as city,
    (doc->>'country')::text as country,

    case
      when (doc->>'forecast_dt') ~ '^[0-9]+$' then to_timestamp((doc->>'forecast_dt')::bigint)
      else (doc->>'forecast_dt')::timestamptz
    end as forecast_timestamp,

    case
      when (doc->>'collection_dt') ~ '^[0-9]+$' then to_timestamp((doc->>'collection_dt')::bigint)
      else (doc->>'collection_dt')::timestamptz
    end as mongo_collection_ts,

    (doc->>'temp')::numeric as temp,
    (doc->>'humidity')::int as humidity,
    (doc->'weather'->0->>'main')::text as forecast_weather_main,

    loaded_at as pg_loaded_at
from source
