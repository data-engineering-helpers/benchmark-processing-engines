#!/usr/bin/env python3
"""
Quick test script for DataFusion benchmark
"""
import sys
from pathlib import Path

def test_datafusion_benchmark():
    """Test if DataFusion benchmark runs without errors"""
    try:
        # Import datafusion to check if it's available
        import datafusion
        print("✓ DataFusion is available")
        
        # Try to import and run the benchmark
        sys.path.append(str(Path(__file__).parent / 'datafusion'))
        from benchmark import DataFusionBenchmark
        
        print("✓ DataFusion benchmark module imported successfully")
        
        # Test basic functionality
        benchmark = DataFusionBenchmark()
        print("✓ DataFusionBenchmark instance created")
        
        # Test data loading
        result = benchmark.load_data()
        print(f"✓ Data loaded successfully: {result} records")
        
        # Test simple aggregation
        result = benchmark.filter_and_aggregate()
        print(f"✓ Filter and aggregate completed: {result} results")
        
        print("\n🎉 DataFusion benchmark test passed!")
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Install DataFusion with: pip install datafusion")
        return False
    except Exception as e:
        print(f"❌ Error running DataFusion benchmark: {e}")
        return False

if __name__ == "__main__":
    success = test_datafusion_benchmark()
    sys.exit(0 if success else 1)