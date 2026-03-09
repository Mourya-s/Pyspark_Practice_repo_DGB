# 1️ Setup SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("GeneralDFFunctions").getOrCreate()

# 2️ Create DataFrame
data = [("Alice", 1000), ("Bob", 1500), ("Charlie", 1200)]
df = spark.createDataFrame(data, ["name", "salary"])

# 13.1 show() → display DataFrame
df.show()

# 13.2 collect() → returns list of Row objects
print("Collect:", df.collect())

# 13.3 take(n) → returns first n rows
print("Take 2:", df.take(2))

# 13.4 printSchema() → display schema
df.printSchema()

# 13.5 count() → number of rows
print("Count:", df.count())

# 13.6 select() → select columns
df.select("name").show()

# 13.7 filter() / where() → filter rows
df.filter(col("salary") > 1200).show()
df.where(col("salary") > 1200).show()

# 13.8 like() → pattern matching
df.filter(col("name").like("A%")).show()

# 13.9 sort() → sort by column
df.sort(col("salary").desc()).show()

# 13.10 describe() → summary statistics
df.describe().show()

# 13.11 columns → list of column names
print("Columns:", df.columns)