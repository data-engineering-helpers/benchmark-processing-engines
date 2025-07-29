#!/usr/bin/env python3
"""
Raw Python benchmark implementation using pandas
"""
import pandas as pd
import json
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))
from benchmark_runner import BenchmarkRunner, get_data_paths

class RawPythonBenchmark:
    def __init__(self, scale='medium'):
        self.data_paths = get_data_paths(scale)
        self.profiles_df = None
        self.events_df = None
        self.scale = scale
        
    def load_data(self):
        """Load data from parquet files"""
        self.profiles_df = pd.read_parquet(self.data_paths['profiles'])
        self.events_df = pd.read_parquet(self.data_paths['events'])
        
        # Parse JSON columns - handle both string and dict cases
        self.profiles_df['address_parsed'] = self.profiles_df['address'].apply(
            lambda x: json.loads(x) if isinstance(x, str) else x
        )
        self.profiles_df['preferences_parsed'] = self.profiles_df['preferences'].apply(
            lambda x: json.loads(x) if isinstance(x, str) else x
        )
        self.events_df['event_data_parsed'] = self.events_df['event_data'].apply(
            lambda x: json.loads(x) if isinstance(x, str) else x
        )
        
        return len(self.profiles_df) + len(self.events_df)
    
    def filter_and_aggregate(self):
        """Filter last 7 days and aggregate by customer_id, event_type"""
        # Parse timestamps
        self.events_df['event_timestamp_dt'] = pd.to_datetime(
            self.events_df['event_timestamp'], format='mixed'
        )
        
        # Filter last 7 days from max timestamp
        max_timestamp = self.events_df['event_timestamp_dt'].max()
        seven_days_ago = max_timestamp - pd.Timedelta(days=7)
        recent_events = self.events_df[
            self.events_df['event_timestamp_dt'] >= seven_days_ago
        ]
        
        # Aggregate by customer_id and event_type
        result = recent_events.groupby(['customer_id', 'event_type']).agg({
            'event_id': 'count',
            'event_timestamp_dt': ['min', 'max']
        }).reset_index()
        
        return len(result)
    
    def join_datasets(self):
        """Inner join profiles with events"""
        joined = self.events_df.merge(
            self.profiles_df[['profile_id', 'email_status']], 
            left_on='customer_id', 
            right_on='profile_id',
            how='inner'
        )[['event_id', 'customer_id', 'event_type', 'email_status']]
        
        return len(joined)
    
    def complex_analytics(self):
        """Calculate customer activity metrics"""
        # Count total events per customer
        event_counts = self.events_df.groupby('customer_id').size().reset_index(name='total_events')
        
        # Count login events per customer
        login_events = self.events_df[self.events_df['event_type'] == 'login']
        login_counts = login_events.groupby('customer_id').size().reset_index(name='login_events')
        
        # Merge and calculate metrics
        analytics = event_counts.merge(login_counts, on='customer_id', how='left')
        analytics['login_events'] = analytics['login_events'].fillna(0)
        analytics['login_success_rate'] = 0.9  # Standard assumption
        analytics['activity_score'] = analytics['total_events'] * analytics['login_success_rate']
        
        # Join with profiles for email_status
        final_analytics = analytics.merge(
            self.profiles_df[['profile_id', 'email_status']], 
            left_on='customer_id', 
            right_on='profile_id',
            how='inner'
        )[['customer_id', 'total_events', 'login_events', 'login_success_rate', 'activity_score', 'email_status']]
        
        return len(final_analytics)
    
    def write_results(self):
        """Aggregate by event_type and write results"""
        output_dir = Path(__file__).parent / 'output'
        output_dir.mkdir(exist_ok=True)
        
        # Aggregate by event_type
        summary = self.events_df.groupby('event_type').size().reset_index(name='count')
        
        # Write to parquet
        summary.to_parquet(output_dir / 'event_summary.parquet', index=False)
        
        return len(summary)

def run_benchmark(scale='medium'):
    """Run the raw Python benchmark"""
    runner = BenchmarkRunner(scale)
    benchmark = RawPythonBenchmark(scale)
    
    # Run all benchmark tasks
    runner.run_benchmark(f"raw_python_{scale}", "load_data", benchmark.load_data)
    runner.run_benchmark(f"raw_python_{scale}", "filter_and_aggregate", benchmark.filter_and_aggregate)
    runner.run_benchmark(f"raw_python_{scale}", "join_datasets", benchmark.join_datasets)
    runner.run_benchmark(f"raw_python_{scale}", "complex_analytics", benchmark.complex_analytics)
    runner.run_benchmark(f"raw_python_{scale}", "write_results", benchmark.write_results)
    
    runner.print_results()
    return runner.results

if __name__ == "__main__":
    run_benchmark()