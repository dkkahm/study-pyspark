import os, sys
import re

from pyspark.sql import *
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import StringType

from lib.logger import Log4j
from lib.utils import get_spark_app_config


def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"


if __name__ == '__main__':
    conf = get_spark_app_config()

    spark = SparkSession.builder\
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/survey.csv")
    survey_df.show(10)

    parse_gender_udf = udf(parse_gender, StringType())
    logger.info("Category Entry:")
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]
    survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
    survey_df2.show()

    spark.udf.register("parse_gender_udf", parse_gender, StringType())
    logger.info("Category Entry:")
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]

    survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
    survey_df3.show()

    spark.stop()



