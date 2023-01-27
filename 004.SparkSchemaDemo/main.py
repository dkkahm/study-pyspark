import os, sys

from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType

from lib.logger import Log4j
from lib.utils import get_spark_app_config

if __name__ == '__main__':
    conf = get_spark_app_config()

    spark = SparkSession.builder\
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)

    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
              ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
              WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""

    # flightTimeCsvDF = spark.read \
    #     .format("csv") \
    #     .option("header", "true") \
    #     .schema(flightSchemaStruct) \
    #     .option("mode", "FAILFAST") \
    #     .option("dateFormat", "M/d/y") \
    #     .load("data/flight*.csv")
    #
    # flightTimeCsvDF.show()
    # logger.info("CSV Schema: " + flightTimeCsvDF.schema.simpleString())

    flightTimeJsonDF = spark.read \
        .format("json") \
        .schema(flightSchemaDDL) \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "M/d/y") \
        .load("data/flight*.json")

    flightTimeJsonDF.show()
    logger.info("JSON Schema: " + flightTimeJsonDF.schema.simpleString())

    # flightTimeParquetDF = spark.read \
    #     .format("parquet") \
    #     .load("data/flight*.parquet")
    #
    # flightTimeParquetDF.show()
    # logger.info("Parquet Schema: " + flightTimeParquetDF.schema.simpleString())

    spark.stop()



