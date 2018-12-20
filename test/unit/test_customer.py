import os
import sys
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from .test_environment_setup import SPARK, log
from scd import *

BASE_PATH = os.path.dirname(os.path.abspath(__file__)) + "/../data/"

def test_first_insert(request):
    """
        test to check the versioning of a simple table against empty destination
    """
    log("... test to check the versioning of a simple table against empty destination ")
    base_path = BASE_PATH + "{}/{}/".format(
        __name__.split('.')[-1].replace('test_', ''),
        request.node.name.replace('test_', ''))

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
    actual_df = f(input_df)
    actual_df.show()

    assert actual_df.orderBy("customer_id").collect() \
            == expected_df.orderBy("customer_id").collect()

def test_first_insert_b(request):
    """
        test to check the versioning of a simple table against empty destination
    """
    log("... test to check the versioning of a simple table against empty destination ")
    base_path = BASE_PATH + "{}/{}/".format(
        __name__.split('.')[-1].replace('test_', ''),
        request.node.name.replace('test_', ''))

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
    actual_df = f(input_df)
    actual_df.show()

    assert actual_df.orderBy("customer_id").collect() \
            == expected_df.orderBy("customer_id").collect()
