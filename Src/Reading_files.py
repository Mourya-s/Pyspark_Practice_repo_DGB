from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("ReadCSVWithSchema").getOrCreate()

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Read CSV file with schema
df_csv = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("path/to/file.csv")

df_csv.show()


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadParquet").getOrCreate()

# Read Parquet file (schema is inferred automatically)
df_parquet = spark.read.parquet("path/to/file.parquet")

df_parquet.show()


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("ReadJSONWithSchema").getOrCreate()

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Read JSON file with schema
df_json = spark.read.format("json") \
    .schema(schema) \
    .load("path/to/file.json")

df_json.show()