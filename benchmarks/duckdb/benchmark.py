#!/usr/bin/env python3
"""
DuckDB benchmark implementation
"""
import duckdb
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))
from benchmark_runner import BenchmarkRunner, get_data_paths

class DuckDBBenchmark:
    def __init__(self):
        self.data_paths = get_data_paths()
        self.conn = duckdb.connect()
        self.profiles_loaded = False
        self.events_loaded = False
        
    def load_data(self):
        """Load data from parquet files into DuckDB"""
        # Create tables from parquet files
        self.conn.execute(f"""
            CREATE TABLE profiles AS 
            SELECT * FROM read_parquet('{self.data_paths["profiles"]}')
        """)
        
        self.conn.execute(f"""
            CREATE TABLE events AS 
            SELECT * FROM read_parquet('{self.data_paths["events"]}')
        """)
        
        # Get row counts
        profiles_count = self.conn.execute("SELECT COUNT(*) FROM profiles").fetchone()[0]
        events_count = self.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        
        self.profiles_loaded = True
        self.events_loaded = True
        
        return profiles_count + events_count
    
    def filter_and_aggregate(self):
        """Filter events and create aggregations"""
        result = self.conn.execute("""
            WITH recent_events AS (
                SELECT *
                FROM events
                WHERE TRY_STRPTIME(event_timestamp, '%Y-%m-%dT%H:%M:%S.%f') >= 
                      (SELECT MAX(TRY_STRPTIME(event_timestamp, '%Y-%m-%dT%H:%M:%S.%f')) - INTERVAL 7 DAYS FROM events)
            )
            SELECT 
                customer_id,
                event_type,
                COUNT(*) as event_count,
                MIN(TRY_STRPTIME(event_timestamp, '%Y-%m-%dT%H:%M:%S.%f')) as first_event,
                MAX(TRY_STRPTIME(event_timestamp, '%Y-%m-%dT%H:%M:%S.%f')) as last_event
            FROM recent_events
            GROUP BY customer_id, event_type
        """).fetchall()
        
        return len(result)
    
    def join_datasets(self):
        """Join profiles with events"""
        result = self.conn.execute("""
            SELECT COUNT(*)
            FROM events e
            INNER JOIN profiles p ON e.customer_id = p.profile_id
        """).fetchone()[0]
        
        return result
    
    def complex_analytics(self):
        """Perform complex analytics operations"""
        result = self.conn.execute("""
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
                    COUNT(*) as login_count,
                    0.9 as success_rate  -- Simplified for demo
                FROM events
                WHERE event_type = 'login'
                GROUP BY customer_id
            )
            SELECT 
                ec.customer_id,
                ec.event_count,
                COALESCE(ls.success_rate, 1.0) as success_rate,
                ec.event_count * COALESCE(ls.success_rate, 1.0) as activity_score
            FROM event_counts ec
            LEFT JOIN login_stats ls ON ec.customer_id = ls.customer_id
        """).fetchall()
        
        return len(result)
    
    def write_results(self):
        """Write results to output files"""
        output_dir = Path(__file__).parent / 'output'
        output_dir.mkdir(exist_ok=True)
        
        # Create summary and export
        self.conn.execute(f"""
            COPY (
                SELECT 
                    event_type,
                    COUNT(*) as count
                FROM events
                GROUP BY event_type
            ) TO '{output_dir}/event_summary.parquet' (FORMAT PARQUET)
        """)
        
        # Get count for return
        result = self.conn.execute("""
            SELECT COUNT(DISTINCT event_type) FROM events
        """).fetchone()[0]
        
        return result
    
    def __del__(self):
        """Clean up connection"""
        if hasattr(self, 'conn'):
            self.conn.close()

def run_benchmark():
    """Run the DuckDB benchmark"""
    runner = BenchmarkRunner()
    benchmark = DuckDBBenchmark()
    
    # Run all benchmark tasks
    runner.run_benchmark("duckdb", "load_data", benchmark.load_data)
    runner.run_benchmark("duckdb", "filter_and_aggregate", benchmark.filter_and_aggregate)
    runner.run_benchmark("duckdb", "join_datasets", benchmark.join_datasets)
    runner.run_benchmark("duckdb", "complex_analytics", benchmark.complex_analytics)
    runner.run_benchmark("duckdb", "write_results", benchmark.write_results)
    
    runner.print_results()
    return runner.results

if __name__ == "__main__":
    run_benchmark()