# 1️ Setup SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, lower, trim, ltrim, rtrim, substring_index, substring, split, repeat, rpad, lpad, regexp_replace, regexp_extract, length, instr, initcap, col, lit

spark = SparkSession.builder.appName("StringFunctions").getOrCreate()

# 2️ Sample DataFrame
data = [("  Alice  ", "alice@gmail.com"), (" Bob ", "bob123@yahoo.com"), ("Charlie", "charlie@xyz.com")]
df = spark.createDataFrame(data, ["name", "email"])

# 14.1 upper()
df.select(upper("name").alias("upper_name")).show()

# 14.2 trim()
df.select(trim("name").alias("trim_name")).show()

# 14.3 ltrim()
df.select(ltrim("name").alias("ltrim_name")).show()

# 14.4 rtrim()
df.select(rtrim("name").alias("rtrim_name")).show()

# 14.5 substring_index()
df.select(substring_index("email", "@", 1).alias("user")).show()  # left of "@"

# 14.6 substring()
df.select(substring("name", 1, 3).alias("sub_name")).show()  # first 3 chars

# 14.7 split()
df.select(split("email", "@").alias("split_email")).show(truncate=False)

# 14.8 repeat()
df.select(repeat("name", 2).alias("repeat_name")).show(truncate=False)

# 14.9 rpad()
df.select(rpad("name", 10, "*").alias("rpad_name")).show()

# 14.10 lpad()
df.select(lpad("name", 10, "*").alias("lpad_name")).show()

# 14.11 regex_replace()
df.select(regexp_replace("email", "[0-9]", "*").alias("regex_replace_email")).show()

# 14.12 lower()
df.select(lower("name").alias("lower_name")).show()

# 14.13 regex_extract()
df.select(regexp_extract("email", "([a-z]+)@", 1).alias("regex_extract_user")).show()

# 14.14 length()
df.select(length("name").alias("name_length")).show()

# 14.15 instr()
df.select(instr("email", "gmail").alias("instr_gmail")).show()  # position of "gmail"

# 14.16 initcap()
df.select(initcap("name").alias("initcap_name")).show()