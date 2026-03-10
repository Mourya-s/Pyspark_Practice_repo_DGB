from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_date, current_timestamp, date_add, datediff, 
    year, month, dayofmonth, to_date, date_format
)

# Create Spark session
spark = SparkSession.builder.appName("DateFunctionsExample").getOrCreate()


# Sample DataFrame with a date string

data = [("2026-03-01",), ("2026-03-05",)]
df = spark.createDataFrame(data, ["date_str"])


# Convert string column to proper date t
df = df.withColumn("date_col", to_date("date_str", "yyyy-MM-dd"))


# Apply various date and time functions

df_result = df.select(
    # Original string column
    "date_str",
    
    # Converted date column
    "date_col",
    
    # Current system date
    current_date().alias("current_date"),
    
    # Current system timestamp
    current_timestamp().alias("current_timestamp"),
    
    # Add 5 days to the date
    date_add("date_col", 5).alias("date_plus_5"),
    
    # Difference in days between current date and date_col
    datediff(current_date(), "date_col").alias("days_diff"),
    
    # Extract year from date
    year("date_col").alias("year"),
    
    # Extract month from date
    month("date_col").alias("month"),
    
    # Extract day of the month
    dayofmonth("date_col").alias("day"),
    
    # Format date in dd/MM/yyyy format
    date_format("date_col", "dd/MM/yyyy").alias("formatted_date")
)


df_result.show(truncate=False)