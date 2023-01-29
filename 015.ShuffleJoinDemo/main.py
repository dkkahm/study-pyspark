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
        .getOrCreate()

    logger = Log4j(spark)

    flight_time_df1 = spark.read.json("data/d1/")
    flight_time_df2 = spark.read.json("data/d2/")

    # logger.info(flight_time_df1.rdd.getNumPartitions())
    # logger.info(flight_time_df2.rdd.getNumPartitions())
    #
    # flight_time_df1.groupBy(f.spark_partition_id()).count().show()
    # flight_time_df2.groupBy(f.spark_partition_id()).count().show()

    join_expr = flight_time_df1.id == flight_time_df2.id
    join_df = flight_time_df1.join(flight_time_df2, join_expr, "inner")

    join_df.foreach(lambda f: None)
    input("press a key to stop ...")

    spark.stop()
