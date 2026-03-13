from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import IntegerType, StringType, DoubleType, ArrayType

# Create Spark Session
spark = SparkSession.builder.appName("SchemaExample").getOrCreate()

# 1️Define Schema using StructType and StructField
employee_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("employee_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("skills", ArrayType(StringType()), True)
])

# 2️ Sample Data
data = [
    (1, "James", "IT", 9000.0, ["Python", "Spark"]),
    (2, "Michael", "HR", 8500.0, ["Excel", "Communication"]),
    (3, "Robert", "Finance", 7800.0, ["SQL", "Accounting"])
]

# 3️ Create DataFrame with Custom Schema
df = spark.createDataFrame(data, schema=employee_schema)

# Show Data
df.show()

# Print Schema
df.printSchema()

# 4️ Write DataFrame into different formats
df.write.mode("overwrite").csv("/data/csv_employees", header=True)
df.write.mode("overwrite").json("/data/json_employees")
df.write.mode("overwrite").parquet("/data/parquet_employees")