# Create an RDD
rdd = sc.parallelize([("Alice", 1000), ("Bob", 1500), ("Charlie", 1200)])

# Convert RDD to DataFrame with column names
df_from_rdd = rdd.toDF(["name", "salary"])
df_from_rdd.show()

# conversion
# Convert DataFrame back to RDD
rdd_from_df = df_from_rdd.rdd
print(rdd_from_df.collect())


# RDD operation
print("RDD salary sum:", rdd_from_df.map(lambda x: x.salary).sum())

# DataFrame operation
df_from_rdd.groupBy().sum("salary").show()


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PySparkExample").getOrCreate()

# RDD from list
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
print("RDD:", rdd.collect())


from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("MyApp").setMaster("local[*]")
sc = SparkContext(conf=conf)

rdd2 = sc.parallelize([10, 20, 30])
print("RDD2:", rdd2.collect())