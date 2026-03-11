from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, rank, dense_rank, lead, lag
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("WindowFunctions").getOrCreate()

data = [
    ("A",100),
    ("A",200),
    ("A",200),
    ("B",300),
    ("B",150)
]

df = spark.createDataFrame(data,["dept","salary"])

windowSpec = Window.partitionBy("dept").orderBy("salary")

result = df.select(
    "dept",
    "salary",
    row_number().over(windowSpec).alias("row_number"),
    rank().over(windowSpec).alias("rank"),
    dense_rank().over(windowSpec).alias("dense_rank"),
    lead("salary").over(windowSpec).alias("lead_salary"),
    lag("salary").over(windowSpec).alias("lag_salary")
)

result.show()