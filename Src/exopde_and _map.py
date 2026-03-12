from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, explode_outer, posexplode_outer

# Create Spark Session
spark = SparkSession.builder.appName("ExplodeFunctionsExample").getOrCreate()

# Sample Data containing arrays and null values
data = [
    (1, ["Apple", "Banana", "Orange"]),
    (2, ["Mango", "Pineapple"]),
    (3, None)
]

columns = ["id", "fruits"]

df = spark.createDataFrame(data, columns)

print("Original DataFrame")
df.show()

# 1. explode()
print("Using explode()")
df_explode = df.select("id", explode("fruits").alias("fruit"))
df_explode.show()

# 2. explode_outer()
print("Using explode_outer()")
df_explode_outer = df.select("id", explode_outer("fruits").alias("fruit"))
df_explode_outer.show()

# 3. posexplode_outer()
print("Using posexplode_outer()")
df_posexplode_outer = df.select("id", posexplode_outer("fruits").alias("position", "fruit"))
df_posexplode_outer.show()