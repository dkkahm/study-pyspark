import os, sys
import re

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
from pyspark.sql.functions import expr

from lib.logger import Log4j
from lib.utils import get_spark_app_config


if __name__ == '__main__':
    conf = get_spark_app_config()

    spark = SparkSession.builder\
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4j(spark)

    """
    df1 = spark.read.json("data/d1/")
    df2 = spark.read.json("data/d2/")

    # df1.show(10)
    # df2.show(10)

    spark.sql("CREATE DATABASE IF NOT EXISTS MY_DB")
    spark.sql("USE MY_DB")

    df1.coalesce(1).write \
        .bucketBy(3, "id") \
        .mode("overwrite") \
        .saveAsTable("MY_DB.flight_data1")

    df2.coalesce(1).write \
        .bucketBy(3, "id") \
        .mode("overwrite") \
        .saveAsTable("MY_DB.flight_data2")
    """

    df3 = spark.read.table("MY_DB.flight_data1")
    df4 = spark.read.table("MY_DB.flight_data2")

    # df3.show(10)
    # df4.show(10)

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    join_expr = df3.id == df4.id
    join_df = df3.join(df4, join_expr, "inner")

    join_df.collect()
    input("press a key...")

    spark.stop()
