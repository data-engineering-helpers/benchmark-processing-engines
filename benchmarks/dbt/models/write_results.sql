{{ config(materialized='table') }}

-- Aggregate by event_type for final output
SELECT 
    event_type,
    COUNT(event_id) as count
FROM {{ ref('stg_events') }}
GROUP BY event_type
ORDER BY event_type