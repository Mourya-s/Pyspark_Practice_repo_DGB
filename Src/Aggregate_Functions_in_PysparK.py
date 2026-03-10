from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    mean, avg, collect_list, collect_set, countDistinct, count,
    first, last, max, min, sum
)


# Create Spark session

spark = SparkSession.builder.appName("AggregateFunctionsExample").getOrCreate()


# Sample DataFrame

data = [
    (1, "Alice", 100),
    (2, "Bob", 200),
    (3, "Alice", 150),
    (4, "Bob", 200),
    (5, "Charlie", 300)
]
df = spark.createDataFrame(data, ["id", "name", "amount"])


# Apply aggregate functions

df_agg = df.select(
    mean("amount").alias("mean_amount"),
                            # Average of column
    avg("amount").alias("avg_amount"),   
                          # Another way to compute average
    collect_list("name").alias("all_names"),
                    # Collect all values into a list
    collect_set("name").alias("unique_names"),
                # Collect unique values into a set
    countDistinct("name").alias("distinct_count"),
      # Count of distinct names
    count("name").alias("total_count"), 
                          # Count of all rows
    first("amount").alias("first_amount"), 
                    # First value of the column
    last("amount").alias("last_amount"),
                          # Last value of the column
    max("amount").alias("max_amount"), 
                            # Maximum value
    min("amount").alias("min_amount"),  
                          # Minimum value
    sum("amount").alias("total_amount") 
                          # Sum of values
)

# Show the results
df_agg.show(truncate=False)