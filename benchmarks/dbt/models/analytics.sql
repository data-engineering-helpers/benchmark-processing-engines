{{ config(materialized='table') }}

WITH event_counts AS (
    SELECT 
        customer_id,
        COUNT(*) as event_count
    FROM {{ ref('stg_events') }}
    GROUP BY customer_id
),

recent_events AS (
    SELECT 
        customer_id,
        event_type,
        COUNT(*) as recent_event_count
    FROM {{ ref('stg_events') }}
    WHERE event_timestamp >= (
        SELECT MAX(event_timestamp) - INTERVAL 7 DAYS 
        FROM {{ ref('stg_events') }}
    )
    GROUP BY customer_id, event_type
),

customer_analytics AS (
    SELECT 
        ec.customer_id,
        ec.event_count,
        p.email_status,
        COALESCE(re.recent_event_count, 0) as recent_activity
    FROM event_counts ec
    LEFT JOIN {{ ref('stg_profiles') }} p ON ec.customer_id = p.profile_id
    LEFT JOIN recent_events re ON ec.customer_id = re.customer_id
)

SELECT * FROM customer_analytics