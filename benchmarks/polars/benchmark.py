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
    def __init__(self, scale='medium'):
        self.data_paths = get_data_paths(scale)
        self.profiles_df = None
        self.events_df = None
        self.scale = scale
        
    def load_data(self):
        """Load data from parquet files"""
        self.profiles_df = pl.read_parquet(self.data_paths['profiles'])
        self.events_df = pl.read_parquet(self.data_paths['events'])
        
        return len(self.profiles_df) + len(self.events_df)
    
    def filter_and_aggregate(self):
        """Filter last 7 days and aggregate by customer_id, event_type"""
        # Parse timestamps
        events_with_ts = self.events_df.with_columns([
            pl.col("event_timestamp").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f", strict=False).alias("event_timestamp_dt")
        ])
        
        # Filter last 7 days from max timestamp
        max_date = events_with_ts.select(pl.col("event_timestamp_dt").max()).item()
        seven_days_ago = max_date - pl.duration(days=7)
        
        recent_events = events_with_ts.filter(
            pl.col("event_timestamp_dt") >= seven_days_ago
        )
        
        # Aggregate by customer_id and event_type
        result = recent_events.group_by(["customer_id", "event_type"]).agg([
            pl.col("event_id").count().alias("event_count"),
            pl.col("event_timestamp_dt").min().alias("min_timestamp"),
            pl.col("event_timestamp_dt").max().alias("max_timestamp")
        ])
        
        return len(result)
    
    def join_datasets(self):
        """Inner join profiles with events"""
        joined = self.events_df.join(
            self.profiles_df.select(["profile_id", "email_status"]),
            left_on="customer_id",
            right_on="profile_id",
            how="inner"
        ).select(["event_id", "customer_id", "event_type", "email_status"])
        
        return len(joined)
    
    def complex_analytics(self):
        """Calculate customer activity metrics"""
        # Count total events per customer
        event_counts = self.events_df.group_by("customer_id").agg([
            pl.col("event_id").count().alias("total_events")
        ])
        
        # Count login events per customer
        login_events = self.events_df.filter(pl.col("event_type") == "login")
        login_counts = login_events.group_by("customer_id").agg([
            pl.col("event_id").count().alias("login_events")
        ])
        
        # Merge and calculate metrics
        analytics = event_counts.join(login_counts, on="customer_id", how="left")
        analytics = analytics.with_columns([
            pl.col("login_events").fill_null(0),
            pl.lit(0.9).alias("login_success_rate"),
        ])
        analytics = analytics.with_columns([
            (pl.col("total_events") * pl.col("login_success_rate")).alias("activity_score")
        ])
        
        # Join with profiles for email_status
        final_analytics = analytics.join(
            self.profiles_df.select(["profile_id", "email_status"]),
            left_on="customer_id",
            right_on="profile_id",
            how="inner"
        ).select(["customer_id", "total_events", "login_events", "login_success_rate", "activity_score", "email_status"])
        
        return len(final_analytics)
    
    def write_results(self):
        """Write results to output files"""
        output_dir = Path(__file__).parent / 'output'
        output_dir.mkdir(exist_ok=True)
        
        # Aggregate by event_type
        summary = self.events_df.group_by("event_type").agg([
            pl.col("event_id").count().alias("count")
        ]).sort("event_type")
        
        # Write to parquet
        summary.write_parquet(output_dir / 'event_summary.parquet')
        return len(summary)

def run_benchmark(scale='medium'):
    """Run the Polars benchmark"""
    runner = BenchmarkRunner(scale)
    benchmark = PolarsBenchmark(scale)
    
    # Run all benchmark tasks
    runner.run_benchmark(f"polars_{scale}", "load_data", benchmark.load_data)
    runner.run_benchmark(f"polars_{scale}", "filter_and_aggregate", benchmark.filter_and_aggregate)
    runner.run_benchmark(f"polars_{scale}", "join_datasets", benchmark.join_datasets)
    runner.run_benchmark(f"polars_{scale}", "complex_analytics", benchmark.complex_analytics)
    runner.run_benchmark(f"polars_{scale}", "write_results", benchmark.write_results)
    
    runner.print_results()
    return runner.results

if __name__ == "__main__":
    run_benchmark()