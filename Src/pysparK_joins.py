from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("JoinsExample").getOrCreate()

# Sample DataFrames

df1 = spark.createDataFrame(
    [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
    ["id", "name"]
)

df2 = spark.createDataFrame(
    [(2, "HR"), (3, "IT"), (4, "Finance")],
    ["id", "department"]
)


inner_join_df = df1.join(df2, on="id", how="inner")
inner_join_df.show()
# Left Join (all rows from left, matching from right)

left_join_df = df1.join(df2, on="id", how="left")
left_join_df.show()


# Right Join (all rows from right, matching from left)

right_join_df = df1.join(df2, on="id", how="right")
right_join_df.show()

# Full Outer Join (all rows from both)

full_join_df = df1.join(df2, on="id", how="outer")
full_join_df.show()

# Left Semi Join (rows from left that have match in right)

left_semi_df = df1.join(df2, on="id", how="left_semi")
left_semi_df.show()

# Left Anti Join (rows from left that do NOT have match in right)

left_anti_df = df1.join(df2, on="id", how="left_anti")
left_anti_df.show()