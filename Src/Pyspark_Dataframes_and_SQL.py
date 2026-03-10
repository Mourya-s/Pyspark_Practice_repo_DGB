from pyspark.sql.functions import col, sum

# Sample DataFrame
data = [("Alice", 1000), ("Bob", 1500), ("Charlie", 1200)]
df = spark.createDataFrame(data, ["name", "salary"])

# Select
df.select("name").show()

# Filter
df.filter(col("salary") > 1200).show()

# Aggregate
df.groupBy().agg(sum("salary").alias("total_salary")).show()

#Pysaprsql
# 1️ Setup SparkSession
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PySparkSQLExample").getOrCreate()

# 2️ Create DataFrame
data = [("Alice", 1000), ("Bob", 1500), ("Charlie", 1200)]
df = spark.createDataFrame(data, ["name", "salary"])

# 3️ Register DataFrame as a temporary SQL table/view
df.createOrReplaceTempView("employee")

# 4️ Run SQL queries using spark.sql()

# Select all rows
spark.sql("SELECT * FROM employee").show()

# Filter rows
spark.sql("SELECT name, salary FROM employee WHERE salary > 1200").show()

# Aggregate
spark.sql("SELECT SUM(salary) as total_salary FROM employee").show()

# Example with ordering
spark.sql("SELECT * FROM employee ORDER BY salary DESC").show()