#!/usr/bin/env python3
"""
Benchmark runner for comparing data processing engines
"""
import time
import psutil
import os
from typing import Dict, Any, Callable
from dataclasses import dataclass
from pathlib import Path

@dataclass
class BenchmarkResult:
    engine: str
    task: str
    execution_time: float
    memory_usage_mb: float
    cpu_percent: float
    success: bool
    error_message: str = ""

class BenchmarkRunner:
    def __init__(self, scale='medium'):
        self.results = []
        self.scale = scale
        
    def run_benchmark(self, engine: str, task: str, func: Callable) -> BenchmarkResult:
        """Run a single benchmark and collect metrics"""
        print(f"Running {engine} - {task}...")
        
        # Get initial memory usage
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        try:
            # Start timing and CPU monitoring
            start_time = time.time()
            cpu_before = psutil.cpu_percent()
            
            # Run the benchmark function
            result = func()
            
            # Calculate metrics
            end_time = time.time()
            execution_time = end_time - start_time
            final_memory = process.memory_info().rss / 1024 / 1024  # MB
            memory_usage = final_memory - initial_memory
            cpu_after = psutil.cpu_percent()
            cpu_usage = max(cpu_after - cpu_before, 0)
            
            benchmark_result = BenchmarkResult(
                engine=engine,
                task=task,
                execution_time=execution_time,
                memory_usage_mb=memory_usage,
                cpu_percent=cpu_usage,
                success=True
            )
            
        except Exception as e:
            benchmark_result = BenchmarkResult(
                engine=engine,
                task=task,
                execution_time=0,
                memory_usage_mb=0,
                cpu_percent=0,
                success=False,
                error_message=str(e)
            )
            
        self.results.append(benchmark_result)
        return benchmark_result
    
    def print_results(self):
        """Print benchmark results in a formatted table"""
        print("\n" + "="*80)
        print("BENCHMARK RESULTS")
        print("="*80)
        print(f"{'Engine':<12} {'Task':<20} {'Time (s)':<10} {'Memory (MB)':<12} {'CPU %':<8} {'Status':<8}")
        print("-"*80)
        
        for result in self.results:
            status = "✓" if result.success else "✗"
            print(f"{result.engine:<12} {result.task:<20} {result.execution_time:<10.3f} "
                  f"{result.memory_usage_mb:<12.1f} {result.cpu_percent:<8.1f} {status:<8}")
            if not result.success:
                print(f"  Error: {result.error_message}")
        
        print("-"*80)
        
        # Summary by engine
        engines = set(r.engine for r in self.results if r.success)
        print("\nSUMMARY BY ENGINE:")
        for engine in sorted(engines):
            engine_results = [r for r in self.results if r.engine == engine and r.success]
            if engine_results:
                avg_time = sum(r.execution_time for r in engine_results) / len(engine_results)
                avg_memory = sum(r.memory_usage_mb for r in engine_results) / len(engine_results)
                print(f"{engine:<12} Avg Time: {avg_time:.3f}s, Avg Memory: {avg_memory:.1f}MB")

# Common benchmark tasks
BENCHMARK_TASKS = [
    "load_data",
    "filter_and_aggregate", 
    "join_datasets",
    "complex_analytics",
    "write_results"
]

def get_data_paths(scale='medium'):
    """Get paths to the sample data files for a specific scale"""
    from data_generator import get_data_paths_for_scale
    return get_data_paths_for_scale(scale)