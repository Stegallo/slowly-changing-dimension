"""
    Test cases
"""
from pyspark.sql import SparkSession

SPARK = SparkSession.builder.enableHiveSupport().appName("unit_test").getOrCreate()

SPARK.sparkContext.setLogLevel("WARN")

def log(message):
    """
        generic log function
    """
    print(message)

def init():
    """
        set up environment for testing
    """
    log("""
        ... environment setup started ...
    """)
    try:
        # ignoring possible exceptions if database does not exists
        SPARK.sql("""
            DROP TABLE IF EXISTS tmp.date_dim
        """)
    except Exception as exc:
        # ignoring possible exceptions if database does not exists
        pass

    SPARK.sql("""
        DROP DATABASE IF EXISTS tmp
    """)
    SPARK.sql("""
        CREATE DATABASE IF NOT EXISTS tmp
    """)

    SPARK.sql("""
        CREATE TABLE IF NOT EXISTS tmp.date_dim AS
        SELECT '43101' AS date_key, '2018-01-01' AS actual_date
         UNION ALL
        SELECT '43102' AS date_key, '2018-01-02' AS actual_date
    """)
    log("""
        ... environment setup completed ...
    """)

init()

def test_1():
    SPARK.sql("""
        SELECT * FROM tmp.date_dim
    """).show()
    assert True
