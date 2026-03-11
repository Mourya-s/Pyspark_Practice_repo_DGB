from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, StringType, BooleanType, DateType, TimestampType

spark = SparkSession.builder.appName("CastExample").getOrCreate()

data = [("1","100.5","true","2024-01-01","2024-01-01 10:00:00")]
cols = ["id","amount","flag","date_str","time_str"]

df = spark.createDataFrame(data,cols)

df_cast = df.select(
    col("id").cast(IntegerType()).alias("id_int"),
    col("amount").cast(DoubleType()).alias("amount_double"),
    col("id").cast(StringType()).alias("id_string"),
    col("flag").cast(BooleanType()).alias("flag_boolean"),
    col("date_str").cast(DateType()).alias("date"),
    col("time_str").cast(TimestampType()).alias("timestamp")
)

df_cast.show()