{{ config(tags=["stg"]) }}

with source as (
    select * from {{ source('weather_raw', 'current_weather') }}
)

select
    id as raw_id,
    (doc->>'city')::text as city,
    (doc->>'country')::text as country,

    case
      when (doc->>'dt') ~ '^[0-9]+$' then to_timestamp((doc->>'dt')::bigint)
      else (doc->>'dt')::timestamptz
    end as observation_dt,

    (doc->>'temp')::numeric as temp,
    (doc->>'feels_like')::numeric as feels_like,
    (doc->>'pressure')::int as pressure,
    (doc->>'humidity')::int as humidity,
    (doc->'weather'->0->>'main')::text as weather_main,

    case
      when (doc->>'collected_ts') ~ '^[0-9]+$' then to_timestamp((doc->>'collected_ts')::bigint)
      else (doc->>'collected_ts')::timestamptz
    end as mongo_collection_ts,

    loaded_at as pg_loaded_at
from source
