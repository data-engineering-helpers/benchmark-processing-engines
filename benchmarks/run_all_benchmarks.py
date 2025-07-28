#!/usr/bin/env python3
"""
Main script to run all data processing engine benchmarks
"""
import sys
import importlib.util
from pathlib import Path
import pandas as pd
from benchmark_runner import BenchmarkRunner

def import_benchmark_module(engine_name):
    """Dynamically import benchmark module for an engine"""
    module_path = Path(__file__).parent / engine_name / 'benchmark.py'
    if not module_path.exists():
        print(f"Warning: Benchmark module not found for {engine_name}")
        return None
    
    spec = importlib.util.spec_from_file_location(f"{engine_name}_benchmark", module_path)
    module = importlib.util.module_from_spec(spec)
    
    try:
        spec.loader.exec_module(module)
        return module
    except Exception as e:
        print(f"Error importing {engine_name} benchmark: {e}")
        return None

def run_single_engine(engine_name):
    """Run benchmark for a single engine"""
    print(f"\n{'='*60}")
    print(f"Running {engine_name.upper()} Benchmark")
    print(f"{'='*60}")
    
    module = import_benchmark_module(engine_name)
    if module and hasattr(module, 'run_benchmark'):
        try:
            return module.run_benchmark()
        except Exception as e:
            print(f"Error running {engine_name} benchmark: {e}")
            return []
    else:
        print(f"No run_benchmark function found for {engine_name}")
        return []

def main():
    """Main function to run all benchmarks"""
    engines = ['raw_python', 'polars', 'duckdb', 'daft', 'datafusion', 'pyspark', 'dbt', 'sqlmesh']
    
    # Allow running specific engines via command line
    if len(sys.argv) > 1:
        engines = [arg for arg in sys.argv[1:] if arg in engines]
        if not engines:
            print("Available engines: raw_python, polars, duckdb, daft, datafusion, pyspark, dbt, sqlmesh")
            return
    
    all_results = []
    
    for engine in engines:
        results = run_single_engine(engine)
        all_results.extend(results)
    
    # Create comprehensive comparison
    if all_results:
        print(f"\n{'='*80}")
        print("COMPREHENSIVE BENCHMARK COMPARISON")
        print(f"{'='*80}")
        
        # Convert to DataFrame for easier analysis
        df = pd.DataFrame([
            {
                'engine': r.engine,
                'task': r.task,
                'execution_time': r.execution_time,
                'memory_usage_mb': r.memory_usage_mb,
                'cpu_percent': r.cpu_percent,
                'success': r.success
            }
            for r in all_results if r.success
        ])
        
        if not df.empty:
            # Summary by engine
            print("\nAVERAGE PERFORMANCE BY ENGINE:")
            print("-" * 50)
            engine_summary = df.groupby('engine').agg({
                'execution_time': 'mean',
                'memory_usage_mb': 'mean',
                'cpu_percent': 'mean'
            }).round(3)
            
            print(engine_summary.to_string())
            
            # Best performer by task
            print("\nBEST PERFORMER BY TASK:")
            print("-" * 40)
            for task in df['task'].unique():
                task_data = df[df['task'] == task]
                if not task_data.empty:
                    fastest = task_data.loc[task_data['execution_time'].idxmin()]
                    print(f"{task:<20} {fastest['engine']:<12} ({fastest['execution_time']:.3f}s)")
            
            # Save detailed results
            output_file = Path(__file__).parent / 'benchmark_results.csv'
            df.to_csv(output_file, index=False)
            print(f"\nDetailed results saved to: {output_file}")
        
        # Show failed benchmarks
        failed_results = [r for r in all_results if not r.success]
        if failed_results:
            print(f"\nFAILED BENCHMARKS ({len(failed_results)}):")
            print("-" * 30)
            for r in failed_results:
                print(f"{r.engine} - {r.task}: {r.error_message}")

if __name__ == "__main__":
    main()