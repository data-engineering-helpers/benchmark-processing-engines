#!/usr/bin/env python3
"""
PySpark benchmark implementation
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))
from benchmark_runner import BenchmarkRunner, get_data_paths

class PySparkBenchmark:
    def __init__(self):
        self.data_paths = get_data_paths()
        self.spark = SparkSession.builder \
            .appName("DataProcessingBenchmark") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Reduce logging
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.profiles_df = None
        self.events_df = None
        
    def load_data(self):
        """Load data from parquet files"""
        self.profiles_df = self.spark.read.parquet(str(self.data_paths['profiles']))
        self.events_df = self.spark.read.parquet(str(self.data_paths['events']))
        
        # Cache for better performance in subsequent operations
        self.profiles_df.cache()
        self.events_df.cache()
        
        return self.profiles_df.count() + self.events_df.count()
    
    def filter_and_aggregate(self):
        """Filter events and create aggregations"""
        # Convert timestamp and filter last 7 days - handle microseconds
        events_with_ts = self.events_df.withColumn(
            "event_timestamp_parsed", 
            to_timestamp(col("event_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
        )
        
        max_date = events_with_ts.agg(max("event_timestamp_parsed")).collect()[0][0]
        seven_days_ago = max_date - expr("INTERVAL 7 DAYS")
        
        recent_events = events_with_ts.filter(
            col("event_timestamp_parsed") >= lit(seven_days_ago)
        )
        
        # Aggregate by customer and event type
        agg_result = recent_events.groupBy("customer_id", "event_type").agg(
            count("event_id").alias("event_count"),
            min("event_timestamp_parsed").alias("first_event"),
            max("event_timestamp_parsed").alias("last_event")
        )
        
        return agg_result.count()
    
    def join_datasets(self):
        """Join profiles with events"""
        profiles_subset = self.profiles_df.select("profile_id", "email_status")
        
        joined = self.events_df.join(
            profiles_subset,
            self.events_df.customer_id == profiles_subset.profile_id,
            "inner"
        )
        
        return joined.count()
    
    def complex_analytics(self):
        """Perform complex analytics operations"""
        # Calculate event counts per customer
        event_counts = self.events_df.groupBy("customer_id").agg(
            count("event_id").alias("event_count")
        )
        
        # Calculate login success rates (simplified)
        login_events = self.events_df.filter(col("event_type") == "login")
        login_success = login_events.groupBy("customer_id").agg(
            count("event_id").alias("login_count"),
            lit(0.9).alias("success_rate")  # Simplified
        )
        
        # Combine metrics
        analytics = event_counts.join(login_success, "customer_id", "left")
        analytics = analytics.withColumn(
            "activity_score",
            col("event_count") * coalesce(col("success_rate"), lit(1.0))
        )
        
        return analytics.count()
    
    def write_results(self):
        """Write results to output files"""
        output_dir = Path(__file__).parent / 'output'
        output_dir.mkdir(exist_ok=True)
        
        # Simple aggregation to write
        summary = self.events_df.groupBy("event_type").agg(
            count("event_id").alias("count")
        )
        
        summary.coalesce(1).write.mode("overwrite").parquet(str(output_dir / 'event_summary.parquet'))
        return summary.count()
    
    def __del__(self):
        """Clean up Spark session"""
        if hasattr(self, 'spark'):
            self.spark.stop()

def run_benchmark():
    """Run the PySpark benchmark"""
    runner = BenchmarkRunner()
    benchmark = PySparkBenchmark()
    
    try:
        # Run all benchmark tasks
        runner.run_benchmark("pyspark", "load_data", benchmark.load_data)
        runner.run_benchmark("pyspark", "filter_and_aggregate", benchmark.filter_and_aggregate)
        runner.run_benchmark("pyspark", "join_datasets", benchmark.join_datasets)
        runner.run_benchmark("pyspark", "complex_analytics", benchmark.complex_analytics)
        runner.run_benchmark("pyspark", "write_results", benchmark.write_results)
        
    finally:
        # Ensure Spark session is stopped
        benchmark.spark.stop()
    
    runner.print_results()
    return runner.results

if __name__ == "__main__":
    run_benchmark()