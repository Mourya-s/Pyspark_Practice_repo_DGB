# PySpark Basics

## 1. Introduction to PySpark
- PySpark is the **Python API for Apache Spark**.
- Used for **big data processing** in distributed environments.

## 2. Spark Architecture
- Spark has a **Driver** and multiple **Executors**.
- Driver coordinates tasks; Executors execute computations.

## 3. Spark Components
- **Driver**, **Executors**, **Cluster Manager**, **RDDs**, **DataFrames**, **SparkSession**.

## 4. SparkSession
- Entry point for PySpark applications.
- Create with:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()
