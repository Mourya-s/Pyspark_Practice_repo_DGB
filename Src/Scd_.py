from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit

spark = SparkSession.builder.appName("SCDType2Example").getOrCreate()

# Existing Dimension Table
existing_data = [
    (1, "James", "IT", "2023-01-01", None, True),
    (2, "Michael", "HR", "2023-01-01", None, True)
]

columns = ["emp_id","emp_name","department","start_date","end_date","is_current"]

existing_df = spark.createDataFrame(existing_data, columns)

# Incoming Data
new_data = [
    (1, "James", "Finance"),   # department changed
    (2, "Michael", "HR"),      # no change
    (3, "Robert", "IT")        # new record
]

incoming_df = spark.createDataFrame(new_data, ["emp_id","emp_name","department"])

# Join existing and new data
joined_df = incoming_df.alias("new") \
    .join(existing_df.alias("old"), "emp_id", "left")

# Identify changed records
changed_records = joined_df.filter(
    (joined_df.department != joined_df.department) | joined_df.emp_name.isNull()
)

# Close old records
expired_records = existing_df.join(changed_records, "emp_id") \
    .withColumn("end_date", current_date()) \
    .withColumn("is_current", lit(False))

# Insert new records
new_records = incoming_df \
    .withColumn("start_date", current_date()) \
    .withColumn("end_date", lit(None)) \
    .withColumn("is_current", lit(True))

# Final Dimension Table
final_df = expired_records.unionByName(new_records)

final_df.show()