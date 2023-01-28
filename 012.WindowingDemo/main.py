import os, sys
import re

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f

from lib.logger import Log4j
from lib.utils import get_spark_app_config


if __name__ == '__main__':
    conf = get_spark_app_config()

    spark = SparkSession.builder\
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)

    summary_df = spark.read.parquet("data/summary.parquet")

    running_total_window = Window.partitionBy("Country") \
        .orderBy("WeekNumber") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    summary_df.withColumn("RunningTotal", f.sum("InvoiceValue").over(running_total_window)) \
        .show()

    spark.stop()



