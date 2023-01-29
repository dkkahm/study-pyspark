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

    orders_list = [("01", "02", 350, 1),
                   ("01", "04", 580, 1),
                   ("01", "07", 320, 2),
                   ("02", "03", 450, 1),
                   ("02", "06", 220, 1),
                   ("03", "01", 195, 1),
                   ("04", "09", 270, 3),
                   ("04", "08", 410, 2),
                   ("05", "02", 350, 1)]

    order_df = spark.createDataFrame(orders_list).toDF("order_id", "prod_id", "unit_price", "qty")

    product_list = [("01", "Scroll Mouse", 250, 20),
                    ("02", "Optical Mouse", 350, 20),
                    ("03", "Wireless Mouse", 450, 50),
                    ("04", "Wireless Keyboard", 580, 50),
                    ("05", "Standard Keyboard", 360, 10),
                    ("06", "16 GB Flash Storage", 240, 100),
                    ("07", "32 GB Flash Storage", 320, 50),
                    ("08", "64 GB Flash Storage", 430, 25)]

    product_df = spark.createDataFrame(product_list).toDF("prod_id", "prod_name", "list_price", "qty")

    # order_df.show()
    # product_df.show()

    # product_renamed_df = product_df.withColumnRenamed("qty", "reorder_qty")

    join_expr = order_df.prod_id == product_df.prod_id

    order_df.join(product_df, join_expr, "inner") \
        .drop(product_df.qty) \
        .select("order_id", "prod_name", "unit_price", "qty") \
        .show()

    spark.stop()



