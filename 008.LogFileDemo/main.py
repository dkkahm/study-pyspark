import os, sys

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.logger import Log4j
from lib.utils import get_spark_app_config

if __name__ == '__main__':
    conf = get_spark_app_config()

    spark = SparkSession.builder\
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)

    file_df = spark.read.text("data/apache_logs.txt")
    file_df.printSchema()

    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'

    logs_df = file_df.select(regexp_extract('value', log_reg, 1).alias('ip'),
                             regexp_extract('value', log_reg, 4).alias('date'),
                             regexp_extract('value', log_reg, 6).alias('request'),
                             regexp_extract('value', log_reg, 10).alias('referrer'))

    logs_df \
        .where("trim(referrer) != '-' ") \
        .withColumn("referrer", substring_index("referrer", "/", 3)) \
        .groupBy("referrer") \
        .count() \
        .show(100, truncate=False)

    spark.stop()



