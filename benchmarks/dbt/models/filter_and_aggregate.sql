{{ config(materialized='table') }}

-- Filter events from last 7 days and aggregate by customer_id, event_type
WITH max_timestamp AS (
    SELECT MAX(event_timestamp_dt) as max_ts
    FROM {{ ref('stg_events') }}
    WHERE event_timestamp_dt IS NOT NULL
),

recent_events AS (
    SELECT 
        e.customer_id,
        e.event_type,
        e.event_id,
        e.event_timestamp_dt
    FROM {{ ref('stg_events') }} e
    CROSS JOIN max_timestamp mt
    WHERE e.event_timestamp_dt >= (mt.max_ts - INTERVAL 7 DAYS)
      AND e.event_timestamp_dt IS NOT NULL
)

SELECT 
    customer_id,
    event_type,
    COUNT(event_id) as event_count,
    MIN(event_timestamp_dt) as min_timestamp,
    MAX(event_timestamp_dt) as max_timestamp
FROM recent_events
GROUP BY customer_id, event_type
ORDER BY customer_id, event_type