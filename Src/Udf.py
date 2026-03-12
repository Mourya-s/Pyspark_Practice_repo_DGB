from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("UDFExample").getOrCreate()

# Sample Data
data = [(1, "john.doe@example.com"), (2, "alice@gmail.com")]
columns = ["id", "email"]
df = spark.createDataFrame(data, columns)

# Define UDF to extract domain from email
def extract_domain(email):
    return email.split("@")[-1]

extract_domain_udf = udf(extract_domain, StringType())

# Apply UDF
df_with_domain = df.withColumn("domain", extract_domain_udf("email"))
df_with_domain.show()