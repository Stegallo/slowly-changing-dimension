import os
import sys
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from test_environment_setup import SPARK, log

BASE_PATH = os.path.dirname(os.path.abspath(__file__)) + "/../data/"

def test_first_insert(request):
    """
        test to check the versioning of a simple table against empty destination
    """
    log("... test to check the versioning of a simple table against empty destination ")
    base_path = BASE_PATH + "{}/{}/".format(__name__.replace('test_', ''), request.node.name.replace('test_', ''))

    log("loading data")
    input_df = SPARK.read.format("csv").load(
        base_path + "input.csv",
        header=True)
    input_df.show()

    expected_df = SPARK.read.format("csv").load(
        base_path + "expected.csv",
        header=True)
    expected_df.show()

    log("executing function")
    # actual_df = f(input_df)
    actual_df = SPARK.createDataFrame(
        [('1', 'Alice', '1900-01-01', '2099-12-31'),
         ('2', 'Bob', '1900-01-01', '2099-12-31')],
        StructType(
            [StructField("customer_id", StringType(), True),
             StructField("customer_name", StringType(), True),
             StructField("effective_start_date", StringType(), True),
             StructField("effective_end_date", StringType(), True)]))
    actual_df.show()

    assert actual_df.orderBy("customer_id").collect() \
            == expected_df.orderBy("customer_id").collect()
