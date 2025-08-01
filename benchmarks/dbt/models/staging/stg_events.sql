{{ config(materialized='table') }}

SELECT 
    event_id,
    customer_id,
    event_type,
    event_timestamp,
    -- Parse timestamp with fallback for different formats
    COALESCE(
        TRY_STRPTIME(event_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
        TRY_STRPTIME(event_timestamp, '%Y-%m-%dT%H:%M:%S')
    ) as event_timestamp_dt,
    source_system,
    event_data
FROM read_parquet('{{ var("events_path") }}')