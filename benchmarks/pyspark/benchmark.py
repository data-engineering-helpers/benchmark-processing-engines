#!/usr/bin/env python3
"""
PySpark benchmark implementation
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import when, try_to_timestamp, to_timestamp, max, min, count, lit, expr, coalesce
from pyspark.sql.types import *
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))
from benchmark_runner import BenchmarkRunner, get_data_paths

class PySparkBenchmark:
    def __init__(self, scale='medium'):
        self.data_paths = get_data_paths(scale)
        
        # Configure Spark based on scale
        builder = SparkSession.builder.appName("DataProcessingBenchmark")
        
        if scale == 'large':
            # Increase memory for large datasets and disable caching
            builder = builder \
                .config("spark.driver.memory", "6g") \
                .config("spark.driver.maxResultSize", "3g") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.adaptive.skewJoin.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.execution.arrow.maxRecordsPerBatch", "5000") \
                .config("spark.sql.inMemoryColumnarStorage.compressed", "true") \
                .config("spark.sql.inMemoryColumnarStorage.batchSize", "5000")
        elif scale == 'medium':
            builder = builder \
                .config("spark.driver.memory", "2g") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        else:  # small
            builder = builder \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        self.spark = builder.getOrCreate()
        
        # Reduce logging
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.profiles_df = None
        self.events_df = None
        self.scale = scale
        
    def load_data(self):
        """Load data from parquet files"""
        self.profiles_df = self.spark.read.parquet(str(self.data_paths['profiles']))
        self.events_df = self.spark.read.parquet(str(self.data_paths['events']))
        
        # Only cache for small/medium datasets to avoid memory issues
        if self.scale != 'large':
            self.profiles_df.cache()
            self.events_df.cache()
        
        # For large datasets, avoid caching to prevent memory issues
        elif self.scale == 'large':
            # Don't cache large datasets to avoid memory pressure
            pass
        
        return self.profiles_df.count() + self.events_df.count()
    
    def filter_and_aggregate(self):
        """Filter last 7 days and aggregate by customer_id, event_type"""
        # Parse timestamps with fallback for different formats
        events_with_ts = self.events_df.withColumn(
            "event_timestamp_dt", 
            coalesce(
                try_to_timestamp(col("event_timestamp"), lit("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")),
                try_to_timestamp(col("event_timestamp"), lit("yyyy-MM-dd'T'HH:mm:ss"))
            )
        )
        
        # Filter last 7 days from max timestamp
        max_date = events_with_ts.agg(max("event_timestamp_dt")).collect()[0][0]
        from datetime import timedelta
        seven_days_ago = max_date - timedelta(days=7)
        
        recent_events = events_with_ts.filter(
            col("event_timestamp_dt") >= lit(seven_days_ago)
        )
        
        # Aggregate by customer_id and event_type
        agg_result = recent_events.groupBy("customer_id", "event_type").agg(
            count("event_id").alias("event_count"),
            min("event_timestamp_dt").alias("min_timestamp"),
            max("event_timestamp_dt").alias("max_timestamp")
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
        """Calculate customer activity metrics"""
        # Count total events per customer
        event_counts = self.events_df.groupBy("customer_id").agg(
            count("event_id").alias("total_events")
        )
        
        # Count login events per customer
        login_events = self.events_df.filter(col("event_type") == "login")
        login_counts = login_events.groupBy("customer_id").agg(
            count("event_id").alias("login_events")
        )
        
        # Join metrics
        analytics = event_counts.join(login_counts, on="customer_id", how="left")
        
        # Add calculated columns
        analytics = analytics.fillna(0, subset=["login_events"]) \
            .withColumn("login_success_rate", lit(0.9)) \
            .withColumn("activity_score", col("total_events") * lit(0.9))
        
        # Join with profiles for email_status
        final_analytics = analytics.join(
            self.profiles_df.select("profile_id", "email_status"),
            analytics.customer_id == self.profiles_df.profile_id,
            "inner"
        ).select(
            "customer_id", "total_events", "login_events", 
            "login_success_rate", "activity_score", "email_status"
        )
        
        return final_analytics.count()
    
    def write_results(self):
        """Aggregate by event_type and write results"""
        output_dir = Path(__file__).parent / 'output'
        output_dir.mkdir(exist_ok=True)
        
        # Aggregate by event_type
        summary = self.events_df.groupBy("event_type").agg(
            count("event_id").alias("count")
        ).orderBy("event_type")
        
        # For large datasets, use more partitions to avoid memory issues
        if self.scale == 'large':
            summary.repartition(4).write.mode("overwrite").parquet(str(output_dir / 'event_summary.parquet'))
        else:
            summary.coalesce(1).write.mode("overwrite").parquet(str(output_dir / 'event_summary.parquet'))
        
        return summary.count()
    
    def cleanup(self):
        """Clean up Spark session and resources"""
        try:
            if hasattr(self, 'profiles_df') and self.profiles_df:
                self.profiles_df.unpersist()
            if hasattr(self, 'events_df') and self.events_df:
                self.events_df.unpersist()
            if hasattr(self, 'spark'):
                self.spark.catalog.clearCache()
                self.spark.stop()
        except Exception:
            pass  # Ignore cleanup errors
    
    def __del__(self):
        """Clean up Spark session"""
        self.cleanup()

def run_benchmark(scale='medium'):
    """Run the PySpark benchmark"""
    runner = BenchmarkRunner(scale)
    benchmark = PySparkBenchmark(scale)
    
    try:
        # Run all benchmark tasks
        runner.run_benchmark(f"pyspark_{scale}", "load_data", benchmark.load_data)
        runner.run_benchmark(f"pyspark_{scale}", "filter_and_aggregate", benchmark.filter_and_aggregate)
        runner.run_benchmark(f"pyspark_{scale}", "join_datasets", benchmark.join_datasets)
        runner.run_benchmark(f"pyspark_{scale}", "complex_analytics", benchmark.complex_analytics)
        runner.run_benchmark(f"pyspark_{scale}", "write_results", benchmark.write_results)
        
    finally:
        # Ensure proper cleanup
        benchmark.cleanup()
    
    runner.print_results()
    return runner.results

if __name__ == "__main__":
    run_benchmark()