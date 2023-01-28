import os, sys

from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4j
from lib.utils import get_spark_app_config

if __name__ == '__main__':
    conf = get_spark_app_config()

    spark = SparkSession.builder\
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/flight*.parquet")

    # logger.info("Num partitions before: " + str(flightTimeParquetDF.rdd.getNumPartitions()))
    # flightTimeParquetDF.groupBy(spark_partition_id()).count().show()
    #
    # partitionedDF = flightTimeParquetDF.repartition(5)
    # logger.info("Num partitions after: " + str(partitionedDF.rdd.getNumPartitions()))
    # partitionedDF.groupBy(spark_partition_id()).count().show()
    #
    # partitionedDF.write \
    #     .format("avro") \
    #     .mode("overwrite") \
    #     .option("path", "dataSink/avro/") \
    #     .save()

    flightTimeParquetDF.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "dataSink/json/") \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("maxRecordsPerFile", 10000) \
        .save()

    spark.stop()



