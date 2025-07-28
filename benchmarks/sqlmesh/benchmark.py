#!/usr/bin/env python3
"""
SQLMesh benchmark implementation
"""
import subprocess
import os
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))
from benchmark_runner import BenchmarkRunner, get_data_paths

class SQLMeshBenchmark:
    def __init__(self):
        self.data_paths = get_data_paths()
        self.sqlmesh_dir = Path(__file__).parent
        
    def load_data(self):
        """Initialize SQLMesh project"""
        result = subprocess.run([
            'sqlmesh', 'init'
        ], capture_output=True, text=True, cwd=self.sqlmesh_dir)
        
        return 1 if result.returncode == 0 else 0
    
    def filter_and_aggregate(self):
        """Run SQLMesh plan"""
        # Set environment variables for data paths
        env = os.environ.copy()
        env['profiles_path'] = str(self.data_paths['profiles'])
        env['events_path'] = str(self.data_paths['events'])
        
        result = subprocess.run([
            'sqlmesh', 'plan', '--auto-apply'
        ], capture_output=True, text=True, cwd=self.sqlmesh_dir, env=env)
        
        return 1 if result.returncode == 0 else 0
    
    def join_datasets(self):
        """Run SQLMesh models"""
        env = os.environ.copy()
        env['profiles_path'] = str(self.data_paths['profiles'])
        env['events_path'] = str(self.data_paths['events'])
        
        result = subprocess.run([
            'sqlmesh', 'run'
        ], capture_output=True, text=True, cwd=self.sqlmesh_dir, env=env)
        
        return 1 if result.returncode == 0 else 0
    
    def complex_analytics(self):
        """Run complex analytics with SQLMesh"""
        # For demo purposes, just run the existing models
        return self.join_datasets()
    
    def write_results(self):
        """Generate SQLMesh documentation"""
        result = subprocess.run([
            'sqlmesh', 'info'
        ], capture_output=True, text=True, cwd=self.sqlmesh_dir)
        
        return 1 if result.returncode == 0 else 0

def run_benchmark():
    """Run the SQLMesh benchmark"""
    runner = BenchmarkRunner()
    benchmark = SQLMeshBenchmark()
    
    # Run all benchmark tasks
    runner.run_benchmark("sqlmesh", "load_data", benchmark.load_data)
    runner.run_benchmark("sqlmesh", "filter_and_aggregate", benchmark.filter_and_aggregate)
    runner.run_benchmark("sqlmesh", "join_datasets", benchmark.join_datasets)
    runner.run_benchmark("sqlmesh", "complex_analytics", benchmark.complex_analytics)
    runner.run_benchmark("sqlmesh", "write_results", benchmark.write_results)
    
    runner.print_results()
    return runner.results

if __name__ == "__main__":
    run_benchmark()