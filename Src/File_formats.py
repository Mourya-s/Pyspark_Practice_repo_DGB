from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("SchemaExample").getOrCreate()

# Define Schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Sample Data
data = [(1, "John", 25), (2, "Alice", 30), (3, "Bob", 28)]

# Create DataFrame with schema
df = spark.createDataFrame(data, schema)
df.show()