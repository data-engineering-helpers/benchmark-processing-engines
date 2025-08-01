#!/usr/bin/env python3
"""
dbt benchmark implementation using SQL models
"""
import subprocess
import os
import duckdb
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))
from benchmark_runner import BenchmarkRunner, get_data_paths

class DBTBenchmark:
    def __init__(self, scale='medium'):
        self.data_paths = get_data_paths(scale)
        self.dbt_dir = Path(__file__).parent
        self.profiles_dir = self.dbt_dir / 'profiles'
        self.scale = scale
        self.conn = duckdb.connect(':memory:')
        
    def _run_dbt_model(self, model_name):
        """Run a specific dbt model and return success status"""
        vars_str = f"profiles_path={self.data_paths['profiles']} events_path={self.data_paths['events']}"
        
        result = subprocess.run([
            'dbt', 'run',
            '--select', model_name,
            '--project-dir', str(self.dbt_dir),
            '--profiles-dir', str(self.profiles_dir),
            '--vars', vars_str
        ], capture_output=True, text=True, cwd=self.dbt_dir)
        
        return result.returncode == 0
    
    def _get_table_count(self, table_name):
        """Get count from a dbt-generated table"""
        try:
            # Connect to the same database dbt uses
            db_path = self.dbt_dir / 'target' / 'benchmark.duckdb'
            if db_path.exists():
                conn = duckdb.connect(str(db_path))
                result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                conn.close()
                return result[0] if result else 0
            return 0
        except Exception:
            return 0
        
    def load_data(self):
        """Load staging data using dbt models"""
        # Run staging models to load and parse data
        staging_success = (
            self._run_dbt_model('stg_profiles') and 
            self._run_dbt_model('stg_events')
        )
        
        if staging_success:
            # Return total records loaded (profiles + events)
            profiles_count = self._get_table_count('stg_profiles')
            events_count = self._get_table_count('stg_events')
            return profiles_count + events_count
        return 0
    
    def filter_and_aggregate(self):
        """Run filter and aggregate model"""
        if self._run_dbt_model('filter_and_aggregate'):
            return self._get_table_count('filter_and_aggregate')
        return 0
    
    def join_datasets(self):
        """Run join datasets model"""
        if self._run_dbt_model('join_datasets'):
            return self._get_table_count('join_datasets')
        return 0
    
    def complex_analytics(self):
        """Run complex analytics model"""
        if self._run_dbt_model('complex_analytics'):
            return self._get_table_count('complex_analytics')
        return 0
    
    def write_results(self):
        """Run write results model and write to parquet"""
        if self._run_dbt_model('write_results'):
            # Export results to parquet file
            output_dir = self.dbt_dir / 'output'
            output_dir.mkdir(exist_ok=True)
            
            try:
                db_path = self.dbt_dir / 'target' / 'benchmark.duckdb'
                if db_path.exists():
                    conn = duckdb.connect(str(db_path))
                    conn.execute(f"""
                        COPY (SELECT * FROM write_results ORDER BY event_type) 
                        TO '{output_dir}/event_summary.parquet' (FORMAT PARQUET)
                    """)
                    conn.close()
                
                return self._get_table_count('write_results')
            except Exception:
                return 0
        return 0

def run_benchmark(scale='medium'):
    """Run the dbt benchmark"""
    runner = BenchmarkRunner(scale)
    benchmark = DBTBenchmark(scale)
    
    # Run all benchmark tasks
    runner.run_benchmark(f"dbt_{scale}", "load_data", benchmark.load_data)
    runner.run_benchmark(f"dbt_{scale}", "filter_and_aggregate", benchmark.filter_and_aggregate)
    runner.run_benchmark(f"dbt_{scale}", "join_datasets", benchmark.join_datasets)
    runner.run_benchmark(f"dbt_{scale}", "complex_analytics", benchmark.complex_analytics)
    runner.run_benchmark(f"dbt_{scale}", "write_results", benchmark.write_results)
    
    runner.print_results()
    return runner.results

if __name__ == "__main__":
    run_benchmark()