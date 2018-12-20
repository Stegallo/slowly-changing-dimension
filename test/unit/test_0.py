from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from .test_environment_setup import SPARK, log

def test_0():
    """
        base test to check the environment
    """
    log("... base test to check the environment ")
    actual_df = SPARK.sql("""
        SELECT * FROM tmp.date_dim
    """)

    expected_df = SPARK.createDataFrame(
        [('43101', '2018-01-01'),
         ('43102', '2018-01-02')],
        StructType(
            [StructField("date_key", StringType(), True),
             StructField("actual_date", StringType(), True)]))

    actual_df.show()
    expected_df.show()

    assert actual_df.orderBy("date_key").collect() \
            == expected_df.orderBy("date_key").collect()
