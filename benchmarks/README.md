# Data Processing Engine Benchmarks

This benchmark suite compares the performance of different data processing engines on common analytical tasks using sample customer data.

## Engines Tested

- **Raw Python** (pandas) - Traditional Python data processing
- **Polars** - Fast DataFrame library written in Rust
- **DuckDB** - In-process analytical database
- **Daft** - Distributed DataFrame library for multimodal data
- **DataFusion** - Apache Arrow's query engine in Rust
- **PySpark** - Distributed computing framework
- **dbt** - Data transformation tool with SQL
- **SQLMesh** - Data transformation framework

## Benchmark Tasks

Each engine performs the same set of tasks:

1. **Load Data** - Read parquet files into memory/engine
2. **Filter & Aggregate** - Filter recent events and create aggregations
3. **Join Datasets** - Join customer profiles with events
4. **Complex Analytics** - Calculate customer activity scores
5. **Write Results** - Export processed data

## Setup

### 1. Generate Sample Data
```bash
cd scripts
python generate-sample-data.py
```

### 2. Install Dependencies
```bash
cd benchmarks
pip install -r requirements.txt
```

### 3. Install Optional Engines
For dbt:
```bash
pip install dbt-core dbt-duckdb
```

For SQLMesh:
```bash
pip install sqlmesh
```

## Running Benchmarks

### Run All Engines
```bash
python run_all_benchmarks.py
```

### Run Specific Engines
```bash
python run_all_benchmarks.py polars duckdb daft datafusion raw_python
```

### Run Individual Engine
```bash
cd raw_python
python benchmark.py
```

## Sample Output

```
================================================================================
COMPREHENSIVE BENCHMARK COMPARISON
================================================================================

AVERAGE PERFORMANCE BY ENGINE:
                execution_time  memory_usage_mb  cpu_percent
engine                                                     
duckdb                   0.145            12.3         15.2
datafusion               0.198            28.7         18.9
polars                   0.234            45.6         25.8
daft                     0.287            52.1         28.3
raw_python               0.456            78.9         35.4
pyspark                  2.134           234.5         45.2

BEST PERFORMER BY TASK:
load_data            duckdb       (0.089s)
filter_and_aggregate polars       (0.156s)
join_datasets        duckdb       (0.123s)
complex_analytics    polars       (0.234s)
write_results        duckdb       (0.098s)
```

## Metrics Collected

- **Execution Time** - Wall clock time for each task
- **Memory Usage** - Peak memory consumption in MB
- **CPU Usage** - CPU utilization percentage
- **Success Rate** - Whether the task completed successfully

## Data Schema

### Customer Profiles
- `profile_id` - Unique customer identifier
- `email` - Customer email address
- `email_status` - Email verification status
- `address` - JSON object with address details
- `preferences` - JSON object with customer preferences

### Customer Events
- `event_id` - Unique event identifier
- `customer_id` - References profile_id
- `event_type` - Type of event (login, logout, etc.)
- `event_timestamp` - When the event occurred
- `source_system` - System that generated the event
- `event_data` - JSON object with event-specific data

## Customization

### Adding New Engines
1. Create a new directory under `benchmarks/`
2. Implement a `benchmark.py` with a `run_benchmark()` function
3. Follow the same task structure as existing engines
4. Add to the engines list in `run_all_benchmarks.py`

### Adding New Tasks
1. Add task function to each engine's benchmark class
2. Update the `BENCHMARK_TASKS` list in `benchmark_runner.py`
3. Call the new task in each engine's `run_benchmark()` function

## Notes

- PySpark may show higher overhead due to JVM startup costs
- dbt and SQLMesh benchmarks measure CLI execution time
- Memory measurements are approximate and may vary by system
- Results will vary based on hardware and data size

## Troubleshooting

### Common Issues
- **Java not found**: Install Java 8+ for PySpark
- **dbt command not found**: Ensure dbt is in your PATH
- **Memory errors**: Reduce data size in generate-sample-data.py
- **Permission errors**: Check file permissions in output directories