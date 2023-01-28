import os, sys

from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4j
from lib.utils import get_spark_app_config

if __name__ == '__main__':
    conf = get_spark_app_config()

    spark = SparkSession.builder\
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/flight*.parquet")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    flightTimeParquetDF.write \
        .format("csv") \
        .mode("overwrite") \
        .bucketBy(5, "ORIGIN", "OP_CARRIER") \
        .sortBy("ORIGIN", "OP_CARRIER") \
        .saveAsTable("flight_data_tbl")

    logger.info(spark.catalog.listTables("AIRLINE_DB"))

    spark.stop()



