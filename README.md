# Knowledge Sharing (KS) - Benchmark of data processing engines

## Table of Content (ToC)

* [Knowledge Sharing (KS) \- Benchmark of data processing engines](#knowledge-sharing-ks---benchmark-of-data-processing-engines)
  * [Table of Content (ToC)](#table-of-content-toc)
  * [Overview](#overview)
  * [References](#references)
  * [Articles](#articles)
    * [Light ETL Python engines](#light-etl-python-engines)
    * [Accelerating Apache Spark's Execution Engine](#accelerating-apache-sparks-execution-engine)
    * [Accelerating Apache Spark with Gluten and Velox](#accelerating-apache-spark-with-gluten-and-velox)
    * [Polars vs DuckDB](#polars-vs-duckdb)

Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc.go)

## Overview

[This project](https://github.com/data-engineering-helpers/benchmark-processing-engines)
aims at benchmarking a few data processing engines (_e.g._, DuckDB, Spark,
Polars, Daft).

Inspiration: [Todo MVC](https://todomvc.com/)

Even though the members of the GitHub organization may be employed by
some companies, they speak on their personal behalf and do not represent
these companies.

## References

* [Todo MVC home page](https://todomvc.com/)
* [Material for the Data platform - Data access to Databricks data lakehouse](https://github.com/data-engineering-helpers/databricks-data-access)
* [Data Engineering Helpers - Knowledge Sharing - Cheat sheets](https://github.com/data-engineering-helpers/ks-cheat-sheets)
  * [Data Engineering Helpers - Knowledge Sharing - Python](https://github.com/data-engineering-helpers/ks-cheat-sheets/tree/main/programming/python)
  * [Data Engineering Helpers - Knowledge Sharing - Spark](https://github.com/data-engineering-helpers/ks-cheat-sheets/blob/main/data-processing/spark/)
  * [Data Engineering Helpers - Knowledge Sharing - DuckDB](https://github.com/data-engineering-helpers/ks-cheat-sheets/blob/main/db/duckdb/)
  * [Data Engineering Helpers - Knowledge Sharing - SQLMesh](https://github.com/data-engineering-helpers/ks-cheat-sheets/blob/main/data-processing/sqlmesh/)
  * [Data Engineering Helpers - Knowledge Sharing - dbt](https://github.com/data-engineering-helpers/ks-cheat-sheets/blob/main/data-processing/dbt/)
* [Material for the Data platform - Architecture principles](https://github.com/data-engineering-helpers/architecture-principles)
* [Material for the Data platform - Modern Data Stack (MDS) in a box](https://github.com/data-engineering-helpers/mds-in-a-box/blob/main/README.md)
* [Material for the Data platform - Data life cycle](https://github.com/data-engineering-helpers/data-life-cycle)
* [Material for the Data platform - Data contracts](https://github.com/data-engineering-helpers/data-contracts)
* [Material for the Data platform - Metadata](https://github.com/data-engineering-helpers/metadata)
* [Material for the Data platform - Data quality](https://github.com/data-engineering-helpers/data-quality)

## Articles

### Light ETL Python engines

* Author: Mimoune Djouallah
  ([Mimoune Djouallah on LinkedIn](https://www.linkedin.com/in/mimounedjouallah),
  [Mimoune Djouallah on GitHub](https://github.com/djouallah))
* Date: Jan. 2026
* [Git repository with a Jupyter notebook and fully reproducible scripts](https://github.com/djouallah/Fabric_Notebooks_Demo/blob/main/ETL/Light_ETL_Python_Notebook.ipynb)
  * [Git repository with the data sets](https://github.com/djouallah/fabric_demo/tree/main/data/archive)
* [Post on LinkedIn](https://www.linkedin.com/posts/mimounedjouallah_python-daft-chdb-activity-7415374580753571840--2-5/)

### Accelerating Apache Spark's Execution Engine

* Title: Accelerating Apache Spark's Execution Engine
* Author: [Dipankar Mazumdar](https://www.linkedin.com/in/dipankar-mazumdar/)
* Date: Dec. 2025
* [Post on LinkedIn](https://www.linkedin.com/posts/dipankar-mazumdar_dataengineering-softwareengineering-activity-7401428947315458048-d7wf/)

### Accelerating Apache Spark with Gluten and Velox

* Title: Accelerating Apache Spark with Gluten & Velox
* Author: Angel Conde
  ([Angel Conde on LinkedIn](https://www.linkedin.com/in/acmanjon/),
  [Angel Conde on Medium](https://medium.com/@neuw84))
* Date: Sep. 2025
* [Link to the article on Medium](https://medium.com/@neuw84/accelerating-apache-spark-with-gluten-velox-3529c4235632)
* [Companion Git repository](https://github.com/Neuw84/spark-gluten-velox).
  It features a benchmark with:
  * Public, generated, datasets containing a fact table and dimension tables
  * Several queries representing typical analytics workload:
    * Query A — Heavy multi‑aggregation: Groups the fact table by `country_id`
    and `channel_id` and computes counts, sums, averages, standard deviation and
    approximate percentiles. This pattern stresses hash aggregation, projection
    and filter operators.
    * Query B — Rollup (cube) aggregation: Joins the fact table with a date
    dimension and uses `rollup(date_key, country_id, product_id)` to compute
    revenue, quantity and average discount across multiple grouping levels.
    * Query C — Star schema join and top‑K sort: Joins the fact table with
    broadcast dimensions (countries and channels) and the date dimension,
    computes gross and net revenue, and orders by gross descending, taking
    the top 5,000 rows. Broadcast joins and top‑K sorts test Velox’s vectorized
    join and sort operators.

### Polars vs DuckDB

* Title: CSV, GZip, S3, Python (Polars vs DuckDB)
* Date: Nov. 2025
* Author: Daniel Beach
  ([Daniel Beach on LinkedIn](https://www.linkedin.com/in/daniel-beach-6ab8b4132/),
  [Daniel Beach on Substack](https://substack.com/@dataengineeringcentral))
* [Link to the article on Substack](https://dataengineeringcentral.substack.com/p/gzip-csv-python-s3-polars-vs-duckdb)
