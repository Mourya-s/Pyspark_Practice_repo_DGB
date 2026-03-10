from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TransformationExample").getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Charlie", 22)]

df = spark.createDataFrame(data, ["name", "age"])

# Transformation
df2 = df.filter(df.age > 23)

# Action
df2.show()

#For select

data = [("Alice", 25), ("Bob", 30)]
df = spark.createDataFrame(data, ["name", "age"])

df.select("name").show()

#map
rdd = spark.sparkContext.parallelize([1,2,3,4])

rdd2 = rdd.map(lambda x: x * 2)

print(rdd2.collect())

#group by
data = [("Alice", "HR"), ("Bob", "IT"), ("Charlie", "IT")]

df = spark.createDataFrame(data, ["name", "dept"])

df.groupBy("dept").count().show()

#count()
data = [("Alice", 25), ("Bob", 30)]

df = spark.createDataFrame(data, ["name", "age"])

print(df.count())