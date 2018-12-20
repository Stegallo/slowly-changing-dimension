"""
    SCD
"""
from pyspark.sql import SparkSession

from pyspark.sql.functions import lit


SPARK = SparkSession.builder.enableHiveSupport().appName("scd").getOrCreate()

SPARK.sparkContext.setLogLevel("WARN")

def f(input_df):
    """
        very first implementation to pass test case
    """
    return input_df \
        .withColumn('effective_start_date', lit('1900-01-01')) \
        .withColumn('effective_end_date', lit('2099-12-31'))
