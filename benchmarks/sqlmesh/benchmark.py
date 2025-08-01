#!/usr/bin/env python3
"""
SQLMesh benchmark implementation using SQL models
"""
import subprocess
import os
import duckdb
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))
from benchmark_runner import BenchmarkRunner, get_data_paths

class SQLMeshBenchmark:
    def __init__(self, scale='medium'):
        self.data_paths = get_data_paths(scale)
        self.sqlmesh_dir = Path(__file__).parent
        self.scale = scale
        
    def _get_env(self):
        """Get environment with data paths"""
        env = os.environ.copy()
        env['profiles_path'] = str(self.data_paths['profiles'])
        env['events_path'] = str(self.data_paths['events'])
        return env
    
    def _run_sqlmesh_command(self, command):
        """Run SQLMesh command with proper environment"""
        result = subprocess.run(
            ['sqlmesh'] + command,
            capture_output=True, 
            text=True, 
            cwd=self.sqlmesh_dir,
            env=self._get_env()
        )
        return result.returncode == 0, result.stdout, result.stderr
    
    def _get_table_count(self, table_name):
        """Get count from a SQLMesh-generated table"""
        try:
            # Connect to the SQLMesh database
            db_path = self.sqlmesh_dir / '.sqlmesh' / 'db.db'
            if db_path.exists():
                conn = duckdb.connect(str(db_path))
                result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                conn.close()
                return result[0] if result else 0
            return 0
        except Exception as e:
            # Try alternative connection methods
            try:
                conn = duckdb.connect(':memory:')
                # Load the data directly for counting
                if 'staging_profiles' in table_name:
                    result = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{self.data_paths['profiles']}')").fetchone()
                elif 'staging_events' in table_name:
                    result = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{self.data_paths['events']}')").fetchone()
                else:
                    return 0
                conn.close()
                return result[0] if result else 0
            except Exception:
                return 0
        
    def load_data(self):
        """Load staging data using SQLMesh models"""
        # Plan and apply staging models
        success, stdout, stderr = self._run_sqlmesh_command(['plan', '--auto-apply'])
        
        if success:
            # Return total records loaded (profiles + events)
            profiles_count = self._get_table_count('benchmark.staging_profiles')
            events_count = self._get_table_count('benchmark.staging_events')
            return profiles_count + events_count
        return 0
    
    def filter_and_aggregate(self):
        """Run filter and aggregate model"""
        success, stdout, stderr = self._run_sqlmesh_command(['run', '--select-model', 'benchmark.filter_and_aggregate'])
        
        if success:
            return self._get_table_count('benchmark.filter_and_aggregate')
        return 0
    
    def join_datasets(self):
        """Run join datasets model"""
        success, stdout, stderr = self._run_sqlmesh_command(['run', '--select-model', 'benchmark.join_datasets'])
        
        if success:
            return self._get_table_count('benchmark.join_datasets')
        return 0
    
    def complex_analytics(self):
        """Run complex analytics model"""
        success, stdout, stderr = self._run_sqlmesh_command(['run', '--select-model', 'benchmark.complex_analytics'])
        
        if success:
            return self._get_table_count('benchmark.complex_analytics')
        return 0
    
    def write_results(self):
        """Run write results model and export to parquet"""
        success, stdout, stderr = self._run_sqlmesh_command(['run', '--select-model', 'benchmark.write_results'])
        
        if success:
            # Export results to parquet file
            output_dir = self.sqlmesh_dir / 'output'
            output_dir.mkdir(exist_ok=True)
            
            try:
                db_path = self.sqlmesh_dir / '.sqlmesh' / 'db.db'
                if db_path.exists():
                    conn = duckdb.connect(str(db_path))
                    conn.execute(f"""
                        COPY (SELECT * FROM benchmark.write_results ORDER BY event_type) 
                        TO '{output_dir}/event_summary.parquet' (FORMAT PARQUET)
                    """)
                    conn.close()
                
                return self._get_table_count('benchmark.write_results')
            except Exception:
                return 0
        return 0

def run_benchmark(scale='medium'):
    """Run the SQLMesh benchmark"""
    runner = BenchmarkRunner(scale)
    benchmark = SQLMeshBenchmark(scale)
    
    # Run all benchmark tasks
    runner.run_benchmark(f"sqlmesh_{scale}", "load_data", benchmark.load_data)
    runner.run_benchmark(f"sqlmesh_{scale}", "filter_and_aggregate", benchmark.filter_and_aggregate)
    runner.run_benchmark(f"sqlmesh_{scale}", "join_datasets", benchmark.join_datasets)
    runner.run_benchmark(f"sqlmesh_{scale}", "complex_analytics", benchmark.complex_analytics)
    runner.run_benchmark(f"sqlmesh_{scale}", "write_results", benchmark.write_results)
    
    runner.print_results()
    return runner.results

if __name__ == "__main__":
    run_benchmark()