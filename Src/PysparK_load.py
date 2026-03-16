from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LoadExample").getOrCreate()

# File path
csv_file = "bigmart.csv"

# Load CSV
df_csv = spark.read.csv(csv_file, header=True, inferSchema=True)

# Show first few rows
df_csv.show()



parquet_file = "titanic.parquet"

df_parquet = spark.read.parquet(parquet_file)

df_parquet.show()




parquet_file = "titanic.parquet"

df_parquet = spark.read.parquet(parquet_file)

df_parquet.show()



jdbc_url = "jdbc:mysql://localhost:3306/mydb"
table_name = "customers"
properties = {"user": "root", "password": "password"}

df_mysql = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

df_mysql.show()