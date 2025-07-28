#!/usr/bin/env python3
"""
dbt benchmark implementation
"""
import subprocess
import os
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))
from benchmark_runner import BenchmarkRunner, get_data_paths

class DBTBenchmark:
    def __init__(self):
        self.data_paths = get_data_paths()
        self.dbt_dir = Path(__file__).parent
        self.profiles_dir = self.dbt_dir / 'profiles'
        
    def load_data(self):
        """Load data using dbt seed or external tables"""
        # dbt doesn't really "load" data in the same way, but we can run deps
        result = subprocess.run([
            'dbt', 'deps', 
            '--project-dir', str(self.dbt_dir),
            '--profiles-dir', str(self.profiles_dir)
        ], capture_output=True, text=True, cwd=self.dbt_dir)
        
        return 1 if result.returncode == 0 else 0
    
    def filter_and_aggregate(self):
        """Run dbt models for filtering and aggregation"""
        # Set variables for data paths
        vars_str = f"profiles_path={self.data_paths['profiles']} events_path={self.data_paths['events']}"
        
        result = subprocess.run([
            'dbt', 'run', 
            '--select', 'stg_events stg_profiles',
            '--project-dir', str(self.dbt_dir),
            '--profiles-dir', str(self.profiles_dir),
            '--vars', vars_str
        ], capture_output=True, text=True, cwd=self.dbt_dir)
        
        return 1 if result.returncode == 0 else 0
    
    def join_datasets(self):
        """Run dbt models that join datasets"""
        vars_str = f"profiles_path={self.data_paths['profiles']} events_path={self.data_paths['events']}"
        
        result = subprocess.run([
            'dbt', 'run',
            '--select', 'analytics',
            '--project-dir', str(self.dbt_dir),
            '--profiles-dir', str(self.profiles_dir),
            '--vars', vars_str
        ], capture_output=True, text=True, cwd=self.dbt_dir)
        
        return 1 if result.returncode == 0 else 0
    
    def complex_analytics(self):
        """Run complex analytics models"""
        # For this demo, we'll just run all models
        vars_str = f"profiles_path={self.data_paths['profiles']} events_path={self.data_paths['events']}"
        
        result = subprocess.run([
            'dbt', 'run',
            '--project-dir', str(self.dbt_dir),
            '--profiles-dir', str(self.profiles_dir),
            '--vars', vars_str
        ], capture_output=True, text=True, cwd=self.dbt_dir)
        
        return 1 if result.returncode == 0 else 0
    
    def write_results(self):
        """Generate documentation and test results"""
        result = subprocess.run([
            'dbt', 'docs', 'generate',
            '--project-dir', str(self.dbt_dir),
            '--profiles-dir', str(self.profiles_dir)
        ], capture_output=True, text=True, cwd=self.dbt_dir)
        
        return 1 if result.returncode == 0 else 0

def run_benchmark():
    """Run the dbt benchmark"""
    runner = BenchmarkRunner()
    benchmark = DBTBenchmark()
    
    # Run all benchmark tasks
    runner.run_benchmark("dbt", "load_data", benchmark.load_data)
    runner.run_benchmark("dbt", "filter_and_aggregate", benchmark.filter_and_aggregate)
    runner.run_benchmark("dbt", "join_datasets", benchmark.join_datasets)
    runner.run_benchmark("dbt", "complex_analytics", benchmark.complex_analytics)
    runner.run_benchmark("dbt", "write_results", benchmark.write_results)
    
    runner.print_results()
    return runner.results

if __name__ == "__main__":
    run_benchmark()