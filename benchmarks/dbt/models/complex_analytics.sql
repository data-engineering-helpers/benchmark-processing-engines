{{ config(materialized='table') }}

-- Calculate customer activity metrics
WITH event_counts AS (
    SELECT 
        customer_id,
        COUNT(event_id) as total_events
    FROM {{ ref('stg_events') }}
    GROUP BY customer_id
),

login_counts AS (
    SELECT 
        customer_id,
        COUNT(event_id) as login_events
    FROM {{ ref('stg_events') }}
    WHERE event_type = 'login'
    GROUP BY customer_id
),

analytics AS (
    SELECT 
        ec.customer_id,
        ec.total_events,
        COALESCE(lc.login_events, 0) as login_events,
        0.9 as login_success_rate,
        ec.total_events * 0.9 as activity_score
    FROM event_counts ec
    LEFT JOIN login_counts lc ON ec.customer_id = lc.customer_id
)

SELECT 
    a.customer_id,
    a.total_events,
    a.login_events,
    a.login_success_rate,
    a.activity_score,
    p.email_status
FROM analytics a
INNER JOIN {{ ref('stg_profiles') }} p ON a.customer_id = p.profile_id
ORDER BY a.customer_id