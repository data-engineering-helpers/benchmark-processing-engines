#!/usr/bin/env python3
"""
Polars benchmark implementation
"""
import polars as pl
import json
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))
from benchmark_runner import BenchmarkRunner, get_data_paths

class PolarsBenchmark:
    def __init__(self):
        self.data_paths = get_data_paths()
        self.profiles_df = None
        self.events_df = None
        
    def load_data(self):
        """Load data from parquet files"""
        self.profiles_df = pl.read_parquet(self.data_paths['profiles'])
        self.events_df = pl.read_parquet(self.data_paths['events'])
        
        return len(self.profiles_df) + len(self.events_df)
    
    def filter_and_aggregate(self):
        """Filter events and create aggregations"""
        # Convert timestamp and filter last 7 days - handle microseconds
        events_with_ts = self.events_df.with_columns([
            pl.col("event_timestamp").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f", strict=False)
        ])
        
        max_date = events_with_ts.select(pl.col("event_timestamp").max()).item()
        seven_days_ago = max_date - pl.duration(days=7)
        
        recent_events = events_with_ts.filter(
            pl.col("event_timestamp") >= seven_days_ago
        )
        
        # Aggregate by customer and event type
        agg_result = recent_events.group_by(["customer_id", "event_type"]).agg([
            pl.col("event_id").count().alias("event_count"),
            pl.col("event_timestamp").min().alias("first_event"),
            pl.col("event_timestamp").max().alias("last_event")
        ])
        
        return len(agg_result)
    
    def join_datasets(self):
        """Join profiles with events"""
        joined = self.events_df.join(
            self.profiles_df.select(["profile_id", "email_status"]),
            left_on="customer_id",
            right_on="profile_id",
            how="inner"
        )
        
        return len(joined)
    
    def complex_analytics(self):
        """Perform complex analytics operations"""
        # Calculate event counts per customer
        event_counts = self.events_df.group_by("customer_id").agg([
            pl.col("event_id").count().alias("event_count")
        ])
        
        # Calculate login success rates
        login_events = self.events_df.filter(pl.col("event_type") == "login")
        
        # For simplicity, assume success rate calculation
        # In real scenario, would parse JSON event_data
        login_success = login_events.group_by("customer_id").agg([
            pl.col("event_id").count().alias("login_count"),
            pl.lit(0.9).alias("success_rate")  # Simplified
        ])
        
        # Combine metrics
        analytics = event_counts.join(login_success, on="customer_id", how="left")
        analytics = analytics.with_columns([
            (pl.col("event_count") * pl.col("success_rate").fill_null(1.0)).alias("activity_score")
        ])
        
        return len(analytics)
    
    def write_results(self):
        """Write results to output files"""
        output_dir = Path(__file__).parent / 'output'
        output_dir.mkdir(exist_ok=True)
        
        # Simple aggregation to write
        summary = self.events_df.group_by("event_type").agg([
            pl.col("event_id").count().alias("count")
        ])
        
        summary.write_parquet(output_dir / 'event_summary.parquet')
        return len(summary)

def run_benchmark():
    """Run the Polars benchmark"""
    runner = BenchmarkRunner()
    benchmark = PolarsBenchmark()
    
    # Run all benchmark tasks
    runner.run_benchmark("polars", "load_data", benchmark.load_data)
    runner.run_benchmark("polars", "filter_and_aggregate", benchmark.filter_and_aggregate)
    runner.run_benchmark("polars", "join_datasets", benchmark.join_datasets)
    runner.run_benchmark("polars", "complex_analytics", benchmark.complex_analytics)
    runner.run_benchmark("polars", "write_results", benchmark.write_results)
    
    runner.print_results()
    return runner.results

if __name__ == "__main__":
    run_benchmark()