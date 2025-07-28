{{ config(materialized='table') }}

SELECT 
    event_id,
    customer_id,
    event_type,
    strptime(event_timestamp, '%Y-%m-%dT%H:%M:%S') as event_timestamp,
    source_system,
    event_data
FROM read_parquet('{{ var("events_path") }}')