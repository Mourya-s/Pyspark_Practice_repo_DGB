from pyspark.sql import SparkSession
from pyspark.sql.functions import abs, ceil, floor, exp, log, pow, sqrt

# Create Spark Session
spark = SparkSession.builder.appName("MathFunctionsExample").getOrCreate()

# Sample Data
data = [(1, -10, 4),
        (2, -5, 9),
        (3, 7, 16)]

columns = ["id", "number", "value"]

df = spark.createDataFrame(data, columns)

# Apply Mathematical Functions
result = df.select(
    "id",
    abs("number").alias("ABS_value"),
    ceil("value").alias("CEIL_value"),
    floor("value").alias("FLOOR_value"),
    exp("id").alias("EXP_value"),
    log("value").alias("LOG_value"),
    pow("value", 2).alias("POWER_value"),
    sqrt("value").alias("SQRT_value")
)

result.show()