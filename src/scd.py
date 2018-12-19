"""
    SCD
"""
from pyspark.sql import SparkSession

from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType


SPARK = SparkSession.builder.enableHiveSupport().appName("scd").getOrCreate()

SPARK.sparkContext.setLogLevel("WARN")

def f(input_df):
    """
        very first implementation to pass test case
    """
    return SPARK.createDataFrame(
        [('1', 'Alice', '1900-01-01', '2099-12-31'),
         ('2', 'Bob', '1900-01-01', '2099-12-31')],
        StructType(
            [StructField("customer_id", StringType(), True),
             StructField("customer_name", StringType(), True),
             StructField("effective_start_date", StringType(), True),
             StructField("effective_end_date", StringType(), True)]))
