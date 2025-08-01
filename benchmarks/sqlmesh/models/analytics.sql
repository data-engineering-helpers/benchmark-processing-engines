MODEL (
  name benchmark.filter_and_aggregate,
  kind FULL
);

-- Filter events from last 7 days and aggregate by customer_id, event_type
WITH max_timestamp AS (
    SELECT MAX(event_timestamp_dt) as max_ts
    FROM benchmark.staging_events
    WHERE event_timestamp_dt IS NOT NULL
),

recent_events AS (
    SELECT 
        e.customer_id,
        e.event_type,
        e.event_id,
        e.event_timestamp_dt
    FROM benchmark.staging_events e
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
ORDER BY customer_id, event_type;

MODEL (
  name benchmark.join_datasets,
  kind FULL
);

-- Inner join events with profiles
SELECT 
    e.event_id,
    e.customer_id,
    e.event_type,
    p.email_status
FROM benchmark.staging_events e
INNER JOIN benchmark.staging_profiles p 
    ON e.customer_id = p.profile_id
ORDER BY e.event_id;

MODEL (
  name benchmark.complex_analytics,
  kind FULL
);

-- Calculate customer activity metrics
WITH event_counts AS (
    SELECT 
        customer_id,
        COUNT(event_id) as total_events
    FROM benchmark.staging_events
    GROUP BY customer_id
),

login_counts AS (
    SELECT 
        customer_id,
        COUNT(event_id) as login_events
    FROM benchmark.staging_events
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
INNER JOIN benchmark.staging_profiles p ON a.customer_id = p.profile_id
ORDER BY a.customer_id;

MODEL (
  name benchmark.write_results,
  kind FULL
);

-- Aggregate by event_type for final output
SELECT 
    event_type,
    COUNT(event_id) as count
FROM benchmark.staging_events
GROUP BY event_type
ORDER BY event_type;