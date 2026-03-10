from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, min, max, round, abs

# Create Spark session
spark = SparkSession.builder.appName("AggregateExample").getOrCreate()

# Sample DataFrame
data = [
    (1, 100),
    (2, 200),
    (3, -50),
    (4, 300)
]
df = spark.createDataFrame(data, ["id", "amount"])

# Aggregations
df_agg = df.select(
    #for the sum of coloumn
    sum("amount").alias("total_amount"),    

    #for the avg of coloumn
    avg("amount").alias("average_amount"),

    #for the min of coloumn
    min("amount").alias("min_amount"),

    #for the max of coloumn
    max("amount").alias("max_amount"),

    #for the round off the coloumn
    round(avg("amount"), 1).alias("avg_rounded"),

    #for the coloumn values removing
    abs(min("amount")).alias("abs_min")
)

df_agg.show()