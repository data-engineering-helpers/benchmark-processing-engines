#!/usr/bin/env python3
"""
Standard operations that all benchmark engines must implement identically
"""

# Standard benchmark operations specification
STANDARD_OPERATIONS = {
    "load_data": {
        "description": "Load customer profiles and events from parquet files",
        "input": ["customer_profiles.parquet", "customer_events.parquet"],
        "output": "Total number of records loaded (profiles + events)",
        "validation": "Return integer count"
    },
    
    "filter_and_aggregate": {
        "description": "Filter events from last 7 days and aggregate by customer_id and event_type",
        "input": "events table",
        "transformation": [
            "1. Parse event_timestamp to datetime",
            "2. Filter events where event_timestamp >= (max_timestamp - 7 days)",
            "3. Group by customer_id, event_type", 
            "4. Count events per group",
            "5. Calculate min and max timestamp per group"
        ],
        "output": "Number of aggregated groups",
        "validation": "Return integer count of groups"
    },
    
    "join_datasets": {
        "description": "Inner join profiles with events on customer_id = profile_id",
        "input": ["profiles table", "events table"],
        "transformation": [
            "1. Inner join events.customer_id = profiles.profile_id",
            "2. Select: event_id, customer_id, event_type, email_status"
        ],
        "output": "Number of joined records",
        "validation": "Return integer count"
    },
    
    "complex_analytics": {
        "description": "Calculate customer activity metrics",
        "input": ["profiles table", "events table"],
        "transformation": [
            "1. Count total events per customer_id",
            "2. Count login events per customer_id", 
            "3. Calculate login success rate (assume 90% for consistency)",
            "4. Calculate activity_score = total_events * login_success_rate",
            "5. Join with profiles to get email_status"
        ],
        "output": "Number of customers with analytics",
        "validation": "Return integer count"
    },
    
    "write_results": {
        "description": "Aggregate events by type and write to parquet",
        "input": "events table",
        "transformation": [
            "1. Group events by event_type",
            "2. Count events per type",
            "3. Write to output/event_summary.parquet"
        ],
        "output": "Number of event types",
        "validation": "Return integer count of distinct event types"
    }
}

def validate_operation_result(operation_name, result, expected_type=int):
    """Validate that an operation returns the expected result type"""
    if not isinstance(result, expected_type):
        raise ValueError(f"Operation {operation_name} must return {expected_type.__name__}, got {type(result).__name__}")
    return result