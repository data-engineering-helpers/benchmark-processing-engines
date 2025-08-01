{{ config(materialized='table') }}

-- Inner join events with profiles
SELECT 
    e.event_id,
    e.customer_id,
    e.event_type,
    p.email_status
FROM {{ ref('stg_events') }} e
INNER JOIN {{ ref('stg_profiles') }} p 
    ON e.customer_id = p.profile_id
ORDER BY e.event_id