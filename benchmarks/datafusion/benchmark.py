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
    def __init__(self):
        self.data_paths = get_data_paths()
        self.ctx = SessionContext()
        self.profiles_loaded = False
        self.events_loaded = False
        
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
        """Filter events and create aggregations"""
        # Simple aggregation by customer and event type
        result = self.ctx.sql("""
            SELECT 
                customer_id,
                event_type,
                COUNT(*) as event_count
            FROM events
            GROUP BY customer_id, event_type
        """).collect()
        
        return len(result)
    
    def join_datasets(self):
        """Join profiles with events"""
        result = self.ctx.sql("""
            SELECT COUNT(*)
            FROM events e
            INNER JOIN profiles p ON e.customer_id = p.profile_id
        """).collect()
        
        return result[0].column(0)[0].as_py()
    
    def complex_analytics(self):
        """Perform complex analytics operations"""
        result = self.ctx.sql("""
            WITH event_counts AS (
                SELECT 
                    customer_id,
                    COUNT(*) as event_count
                FROM events
                GROUP BY customer_id
            ),
            login_stats AS (
                SELECT 
                    customer_id,
                    COUNT(*) as login_count
                FROM events
                WHERE event_type = 'login'
                GROUP BY customer_id
            )
            SELECT 
                ec.customer_id,
                ec.event_count,
                COALESCE(ls.login_count, 0) as login_count,
                ec.event_count * 0.9 as activity_score
            FROM event_counts ec
            LEFT JOIN login_stats ls ON ec.customer_id = ls.customer_id
        """).collect()
        
        return len(result)
    
    def write_results(self):
        """Write results to output files"""
        output_dir = Path(__file__).parent / 'output'
        output_dir.mkdir(exist_ok=True)
        
        # Create summary
        result = self.ctx.sql("""
            SELECT 
                event_type,
                COUNT(*) as count
            FROM events
            GROUP BY event_type
        """)
        
        # Write to parquet
        result.write_parquet(str(output_dir / 'event_summary.parquet'))
        
        # Get count for return
        summary_count = self.ctx.sql("""
            SELECT COUNT(DISTINCT event_type) FROM events
        """).collect()[0].column(0)[0].as_py()
        
        return summary_count

def run_benchmark():
    """Run the DataFusion benchmark"""
    runner = BenchmarkRunner()
    benchmark = DataFusionBenchmark()
    
    # Run all benchmark tasks
    runner.run_benchmark("datafusion", "load_data", benchmark.load_data)
    runner.run_benchmark("datafusion", "filter_and_aggregate", benchmark.filter_and_aggregate)
    runner.run_benchmark("datafusion", "join_datasets", benchmark.join_datasets)
    runner.run_benchmark("datafusion", "complex_analytics", benchmark.complex_analytics)
    runner.run_benchmark("datafusion", "write_results", benchmark.write_results)
    
    runner.print_results()
    return runner.results

if __name__ == "__main__":
    run_benchmark()