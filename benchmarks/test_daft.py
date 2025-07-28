#!/usr/bin/env python3
"""
Quick test script for Daft benchmark
"""
import sys
from pathlib import Path

def test_daft_benchmark():
    """Test if Daft benchmark runs without errors"""
    try:
        # Import daft to check if it's available
        import daft
        print("✓ Daft is available")
        
        # Try to import and run the benchmark
        sys.path.append(str(Path(__file__).parent / 'daft'))
        from benchmark import DaftBenchmark
        
        print("✓ Daft benchmark module imported successfully")
        
        # Test basic functionality
        benchmark = DaftBenchmark()
        print("✓ DaftBenchmark instance created")
        
        # Test data loading
        result = benchmark.load_data()
        print(f"✓ Data loaded successfully: {result} records")
        
        # Test simple aggregation
        result = benchmark.filter_and_aggregate()
        print(f"✓ Filter and aggregate completed: {result} results")
        
        print("\n🎉 Daft benchmark test passed!")
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Install Daft with: pip install getdaft")
        return False
    except Exception as e:
        print(f"❌ Error running Daft benchmark: {e}")
        return False

if __name__ == "__main__":
    success = test_daft_benchmark()
    sys.exit(0 if success else 1)