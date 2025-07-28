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
    def __init__(self):
        self.data_paths = get_data_paths()
        self.profiles_df = None
        self.events_df = None
        
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
        """Filter events and create aggregations"""
        # Filter events from last 7 days
        self.events_df['event_timestamp'] = pd.to_datetime(self.events_df['event_timestamp'])
        recent_events = self.events_df[
            self.events_df['event_timestamp'] >= 
            self.events_df['event_timestamp'].max() - pd.Timedelta(days=7)
        ]
        
        # Aggregate by customer and event type
        agg_result = recent_events.groupby(['customer_id', 'event_type']).agg({
            'event_id': 'count',
            'event_timestamp': ['min', 'max']
        }).reset_index()
        
        return len(agg_result)
    
    def join_datasets(self):
        """Join profiles with events"""
        # Join profiles with events
        joined = self.events_df.merge(
            self.profiles_df[['profile_id', 'email_status']], 
            left_on='customer_id', 
            right_on='profile_id',
            how='inner'
        )
        
        return len(joined)
    
    def complex_analytics(self):
        """Perform complex analytics operations"""
        # Calculate customer activity scores
        event_counts = self.events_df.groupby('customer_id').size().reset_index(name='event_count')
        
        # Get login success rates
        login_events = self.events_df[self.events_df['event_type'] == 'login'].copy()
        login_events['success'] = login_events['event_data_parsed'].apply(
            lambda x: x.get('success', False) if isinstance(x, dict) else False
        )
        
        login_success = login_events.groupby('customer_id')['success'].agg(['count', 'sum']).reset_index()
        login_success['success_rate'] = login_success['sum'] / login_success['count']
        
        # Combine metrics
        analytics = event_counts.merge(login_success, on='customer_id', how='left')
        analytics['activity_score'] = analytics['event_count'] * analytics['success_rate'].fillna(1.0)
        
        return len(analytics)
    
    def write_results(self):
        """Write results to output files"""
        output_dir = Path(__file__).parent / 'output'
        output_dir.mkdir(exist_ok=True)
        
        # Simple aggregation to write
        summary = self.events_df.groupby('event_type').size().reset_index(name='count')
        summary.to_parquet(output_dir / 'event_summary.parquet')
        
        return len(summary)

def run_benchmark():
    """Run the raw Python benchmark"""
    runner = BenchmarkRunner()
    benchmark = RawPythonBenchmark()
    
    # Run all benchmark tasks
    runner.run_benchmark("raw_python", "load_data", benchmark.load_data)
    runner.run_benchmark("raw_python", "filter_and_aggregate", benchmark.filter_and_aggregate)
    runner.run_benchmark("raw_python", "join_datasets", benchmark.join_datasets)
    runner.run_benchmark("raw_python", "complex_analytics", benchmark.complex_analytics)
    runner.run_benchmark("raw_python", "write_results", benchmark.write_results)
    
    runner.print_results()
    return runner.results

if __name__ == "__main__":
    run_benchmark()