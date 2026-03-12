from pyspark.sql import SparkSession
from pyspark.sql.functions import array, array_contains, size, array_position, array_remove

spark = SparkSession.builder.appName("ArrayFunctions").getOrCreate()

data = [
    (1, "A", "B", "C"),
    (2, "A", "C", "D")
]

df = spark.createDataFrame(data,["id","c1","c2","c3"])

df_array = df.select(
    "id",
    array("c1","c2","c3").alias("arr")
)

result = df_array.select(
    "id",
    "arr",
    array_contains("arr","A").alias("contains_A"),
    size("arr").alias("array_length"),
    array_position("arr","C").alias("position_C"),
    array_remove("arr","A").alias("remove_A")
)

result.show()