from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("WriteMethodsExample").getOrCreate()

# Sample Data
data = [
    (1, "James", "IT", 9000),
    (2, "Michael", "HR", 8500),
    (3, "Robert", "Finance", 7800)
]

columns = ["emp_id", "emp_name", "department", "salary"]

df = spark.createDataFrame(data, columns)


# 1️OVERWRITE

df.write \
    .mode("overwrite") \
    .parquet("/data/employees")


# 2️ APPEND
# 
new_data = [
    (4, "Maria", "IT", 9200),
    (5, "David", "HR", 8800)
]

new_df = spark.createDataFrame(new_data, columns)

new_df.write \
    .mode("append") \
    .parquet("/data/employees")

# 3️ OVERWRITE PARTITION
df_partition = df.withColumn("year", col("emp_id") + 2023)

df_partition.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet("/data/employees_partition")

# 4️UPSERT (MERGE)  - using Delta Lake

from delta.tables import DeltaTable

# Load existing table
delta_table = DeltaTable.forPath(spark, "/data/delta/employees")

updates = spark.createDataFrame([
    (1, "James", "IT", 9500),   # updated salary
    (6, "Kevin", "Finance", 8700)  # new record
], columns)

# Merge operation
delta_table.alias("target").merge(
    updates.alias("source"),
    "target.emp_id = source.emp_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()