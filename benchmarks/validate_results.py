#!/usr/bin/env python3
"""
Validation script to ensure all engines produce identical results
"""
import sys
from pathlib import Path
from reference_implementation import get_expected_results
from data_generator import get_data_paths_for_scale

def validate_engine_results(engine_name, scale='small'):
    """Validate that an engine produces expected results"""
    print(f"Validating {engine_name} on {scale} scale...")
    
    # Get expected results
    data_paths = get_data_paths_for_scale(scale)
    expected = get_expected_results(data_paths)
    
    # Import and run the engine
    try:
        import importlib.util
        
        benchmark_path = Path(__file__).parent / engine_name / 'benchmark.py'
        spec = importlib.util.spec_from_file_location(f"{engine_name}_benchmark", benchmark_path)
        benchmark_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(benchmark_module)
        
        # Create benchmark instance
        class_name_map = {
            'raw_python': 'RawPythonBenchmark',
            'duckdb': 'DuckDBBenchmark',
            'polars': 'PolarsBenchmark',
            'daft': 'DaftBenchmark',
            'datafusion': 'DataFusionBenchmark',
            'pyspark': 'PySparkBenchmark',
            'dbt': 'DBTBenchmark',
            'sqlmesh': 'SQLMeshBenchmark'
        }
        
        benchmark_class = getattr(benchmark_module, class_name_map[engine_name])
        benchmark = benchmark_class(scale)
        
        # Run operations and compare results
        results = {}
        operations = ['load_data', 'filter_and_aggregate', 'join_datasets', 'complex_analytics', 'write_results']
        
        for operation in operations:
            try:
                result = getattr(benchmark, operation)()
                results[operation] = result
                
                if result == expected[operation]:
                    print(f"  ✅ {operation}: {result} (matches expected)")
                else:
                    print(f"  ❌ {operation}: {result} (expected {expected[operation]})")
                    return False
            except Exception as e:
                print(f"  ❌ {operation}: ERROR - {e}")
                return False
        
        print(f"  🎉 {engine_name} validation PASSED!")
        return True
        
    except Exception as e:
        print(f"  ❌ Failed to validate {engine_name}: {e}")
        return False

def main():
    """Validate all engines"""
    engines = ['raw_python', 'duckdb', 'polars', 'datafusion']  # All standardized engines
    scale = 'small'
    
    if len(sys.argv) > 1:
        scale = sys.argv[1]
    
    print(f"Validating engines on {scale} scale")
    print("=" * 50)
    
    all_passed = True
    for engine in engines:
        passed = validate_engine_results(engine, scale)
        all_passed = all_passed and passed
        print()
    
    if all_passed:
        print("🎉 All engines validation PASSED!")
    else:
        print("❌ Some engines failed validation!")
        sys.exit(1)

if __name__ == "__main__":
    main()