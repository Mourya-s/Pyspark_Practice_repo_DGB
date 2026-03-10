import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, min, max, round, abs

class TestAggregateFunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create Spark session once for all tests
        cls.spark = SparkSession.builder \
            .appName("AggregateTest") \
            .master("local[1]") \
            .getOrCreate()

        # Sample DataFrame
        data = [(1, 100), (2, 200), (3, -50), (4, 300)]
        cls.df = cls.spark.createDataFrame(data, ["id", "amount"])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_aggregations(self):
        # Perform aggregations
        df_agg = self.df.select(
            sum("amount").alias("total_amount"),
            avg("amount").alias("average_amount"),
            min("amount").alias("min_amount"),
            max("amount").alias("max_amount"),
            round(avg("amount"), 1).alias("avg_rounded"),
            abs(min("amount")).alias("abs_min")
        ).collect()[0]  # collect the row to assert values

        # Assertions
        self.assertEqual(df_agg["total_amount"], 550)
        self.assertAlmostEqual(df_agg["average_amount"], 137.5)
        self.assertEqual(df_agg["min_amount"], -50)
        self.assertEqual(df_agg["max_amount"], 300)
        self.assertAlmostEqual(df_agg["avg_rounded"], 137.5)
        self.assertEqual(df_agg["abs_min"], 50)

if __name__ == "__main__":
    unittest.main()