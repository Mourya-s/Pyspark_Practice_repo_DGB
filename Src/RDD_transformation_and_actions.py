from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDDExample").getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize([1, 2, 3, 4, 5])

#map
rdd = sc.parallelize([1, 2, 3, 4])

rdd2 = rdd.map(lambda x: x * 2)

print(rdd2.collect())

#filter
rdd = sc.parallelize([1, 2, 3, 4, 5])

rdd2 = rdd.filter(lambda x: x > 3)

print(rdd2.collect())

#flat map
rdd = sc.parallelize(["Hello World", "PySpark"])

rdd2 = rdd.flatMap(lambda x: x.split(" "))

print(rdd2.collect())

#first
rdd = sc.parallelize([10, 20, 30])

print(rdd.first())

#count()
rdd = sc.parallelize([1, 2, 3, 4])

print(rdd.count())

#collect()
rdd = sc.parallelize([10, 20, 30])

result = rdd.collect()

print(result)