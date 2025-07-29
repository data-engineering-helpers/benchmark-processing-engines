#!/usr/bin/env python3
"""
DataFusion benchmark implementation
"""
from datafusion import SessionContext
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))
from benchmark_runner import BenchmarkRunner, get_data_paths

class DataFusionBenchmark:
    def __init__(self, scale='medium'):
        self.data_paths = get_data_paths(scale)
        self.ctx = SessionContext()
        self.profiles_loaded = False
        self.events_loaded = False
        self.scale = scale
        
    def load_data(self):
        """Load data from parquet files into DataFusion"""
        # Register parquet files as tables
        self.ctx.register_parquet('profiles', str(self.data_paths['profiles']))
        self.ctx.register_parquet('events', str(self.data_paths['events']))
        
        # Get row counts
        profiles_count = self.ctx.sql("SELECT COUNT(*) FROM profiles").collect()[0].column(0)[0].as_py()
        events_count = self.ctx.sql("SELECT COUNT(*) FROM events").collect()[0].column(0)[0].as_py()
        
        self.profiles_loaded = True
        self.events_loaded = True
        
        return profiles_count + events_count
    
    def filter_and_aggregate(self):
        """Filter last 7 days and aggregate by customer_id, event_type"""
        # DataFusion GROUP BY behaves differently, so we count distinct combinations
        result = self.ctx.sql("""
            WITH parsed_events AS (
                SELECT *,
                       to_timestamp(event_timestamp) as event_ts
                FROM events
            ),
            recent_events AS (
                SELECT customer_id, event_type
                FROM parsed_events
                WHERE event_ts >= (SELECT MAX(event_ts) - INTERVAL '7' DAY FROM parsed_events)
            )
            SELECT COUNT(*) FROM (
                SELECT DISTINCT customer_id, event_type FROM recent_events
            )
        """).collect()
        
        return result[0].column(0)[0].as_py()

    
    def join_datasets(self):
        """Inner join profiles with events"""
        result = self.ctx.sql("""
            SELECT COUNT(*)
            FROM (
                SELECT e.event_id, e.customer_id, e.event_type, p.email_status
                FROM events e
                INNER JOIN profiles p ON e.customer_id = p.profile_id
            )
        """).collect()
        
        return result[0].column(0)[0].as_py()
    
    def complex_analytics(self):
        """Calculate customer activity metrics"""
        result = self.ctx.sql("""
            WITH event_counts AS (
                SELECT 
                    customer_id,
                    COUNT(*) as total_events
                FROM events
                GROUP BY customer_id
            ),
            login_counts AS (
                SELECT 
                    customer_id,
                    COUNT(*) as login_events
                FROM events
                WHERE event_type = 'login'
                GROUP BY customer_id
            )
            SELECT COUNT(*)
            FROM (
                SELECT 
                    ec.customer_id,
                    ec.total_events,
                    COALESCE(lc.login_events, 0) as login_events,
                    0.9 as login_success_rate,
                    ec.total_events * 0.9 as activity_score,
                    p.email_status
                FROM event_counts ec
                LEFT JOIN login_counts lc ON ec.customer_id = lc.customer_id
                INNER JOIN profiles p ON ec.customer_id = p.profile_id
            )
        """).collect()
        
        return result[0].column(0)[0].as_py()
    
    def write_results(self):
        """Aggregate by event_type and write results"""
        output_dir = Path(__file__).parent / 'output'
        output_dir.mkdir(exist_ok=True)
        
        # Create summary
        result = self.ctx.sql("""
            SELECT 
                event_type,
                COUNT(*) as count
            FROM events
            GROUP BY event_type
            ORDER BY event_type
        """)
        
        # Write to parquet
        result.write_parquet(str(output_dir / 'event_summary.parquet'))
        
        # Get count for return
        summary_count = self.ctx.sql("""
            SELECT COUNT(DISTINCT event_type) FROM events
        """).collect()[0].column(0)[0].as_py()
        
        return summary_count

def run_benchmark(scale='medium'):
    """Run the DataFusion benchmark"""
    runner = BenchmarkRunner(scale)
    benchmark = DataFusionBenchmark(scale)
    
    # Run all benchmark tasks
    runner.run_benchmark(f"datafusion_{scale}", "load_data", benchmark.load_data)
    runner.run_benchmark(f"datafusion_{scale}", "filter_and_aggregate", benchmark.filter_and_aggregate)
    runner.run_benchmark(f"datafusion_{scale}", "join_datasets", benchmark.join_datasets)
    runner.run_benchmark(f"datafusion_{scale}", "complex_analytics", benchmark.complex_analytics)
    runner.run_benchmark(f"datafusion_{scale}", "write_results", benchmark.write_results)
    
    runner.print_results()
    return runner.results

if __name__ == "__main__":
    run_benchmark()