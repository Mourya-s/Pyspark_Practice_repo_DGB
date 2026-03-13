from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Create Spark session
spark = SparkSession.builder.appName("WriteFormatsExample").getOrCreate()

# Define Schema
schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("employee_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True)
])

# Sample Data
data = [
    (1, "James", "IT", 9000),
    (2, "Michael", "HR", 8500),
    (3, "Robert", "Finance", 7800)
]

# Create DataFrame with schema
df = spark.createDataFrame(data, schema)

# Write as CSV
df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("/data/output/csv_employees")

# Write as JSON
df.write \
    .mode("overwrite") \
    .json("/data/output/json_employees")

# Write as Parquet (columnar format)
df.write \
    .mode("overwrite") \
    .parquet("/data/output/parquet_employees")

# Write as ORC
df.write \
    .mode("overwrite") \
    .orc("/data/output/orc_employees")

# Write as managed table
df.write \
    .mode("overwrite") \
    .saveAsTable("employees_table")

# Write as external table
df.write \
    .mode("overwrite") \
    .option("path","/data/external/employees") \
    .saveAsTable("employees_external")