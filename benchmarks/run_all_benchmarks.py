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

def run_single_engine(engine_name, scale='medium'):
    """Run benchmark for a single engine at a specific scale"""
    print(f"\n{'='*60}")
    print(f"Running {engine_name.upper()} Benchmark - Scale: {scale}")
    print(f"{'='*60}")
    
    module = import_benchmark_module(engine_name)
    if module and hasattr(module, 'run_benchmark'):
        try:
            return module.run_benchmark(scale)
        except Exception as e:
            print(f"Error running {engine_name} benchmark: {e}")
            return []
    else:
        print(f"No run_benchmark function found for {engine_name}")
        return []

def main():
    """Main function to run all benchmarks"""
    engines = ['raw_python', 'polars', 'duckdb', 'daft', 'datafusion', 'pyspark', 'dbt', 'sqlmesh']
    scales = ['small', 'medium', 'large', 'xlarge']
    
    # Parse command line arguments
    selected_engines = []
    selected_scales = ['medium']  # default scale
    
    for arg in sys.argv[1:]:
        if arg in engines:
            selected_engines.append(arg)
        elif arg in scales:
            selected_scales = [arg]
        elif arg == '--all-scales':
            selected_scales = scales
    
    if not selected_engines:
        selected_engines = engines
    
    if len(sys.argv) > 1 and ('--help' in sys.argv or '-h' in sys.argv):
        print("Usage: python run_all_benchmarks.py [engines...] [scale] [--all-scales]")
        print(f"Available engines: {', '.join(engines)}")
        print(f"Available scales: {', '.join(scales)}")
        print("Examples:")
        print("  python run_all_benchmarks.py small duckdb polars")
        print("  python run_all_benchmarks.py medium")
        print("  python run_all_benchmarks.py large datafusion")
        print("  python run_all_benchmarks.py xlarge duckdb")
        print("  python run_all_benchmarks.py --all-scales duckdb")
        return
    
    print(f"Running benchmarks for scales: {selected_scales}")
    print(f"Running engines: {selected_engines}")
    
    all_results = []
    
    for scale in selected_scales:
        print(f"\n{'='*80}")
        print(f"RUNNING BENCHMARKS FOR SCALE: {scale.upper()}")
        print(f"{'='*80}")
        
        for engine in selected_engines:
            results = run_single_engine(engine, scale)
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
            # Extract scale from engine name (e.g., "duckdb_large" -> "large")
            df['scale'] = df['engine'].str.split('_').str[-1]
            df['engine_name'] = df['engine'].str.rsplit('_', n=1).str[0]
            
            # Group results by scale
            for scale in df['scale'].unique():
                scale_df = df[df['scale'] == scale]
                
                print(f"\n{'='*60}")
                print(f"PERFORMANCE RESULTS FOR {scale.upper()} SCALE")
                print(f"{'='*60}")
                
                # Summary by engine for this scale
                print(f"\nAVERAGE PERFORMANCE BY ENGINE ({scale.upper()}):")
                print("-" * 50)
                engine_summary = scale_df.groupby('engine_name').agg({
                    'execution_time': 'mean',
                    'memory_usage_mb': 'mean',
                    'cpu_percent': 'mean'
                }).round(3).sort_values('execution_time')
                
                print(engine_summary.to_string())
                
                # Best performer by task for this scale
                print(f"\nBEST PERFORMER BY TASK ({scale.upper()}):")
                print("-" * 40)
                for task in scale_df['task'].unique():
                    task_data = scale_df[scale_df['task'] == task]
                    if not task_data.empty:
                        fastest = task_data.loc[task_data['execution_time'].idxmin()]
                        print(f"{task:<20} {fastest['engine_name']:<12} ({fastest['execution_time']:.3f}s)")
            
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