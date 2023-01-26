import sys

from pyspark.sql import *

from lib.logger import Log4j
from lib.utils import get_spark_app_config, load_survey_df

if __name__ == '__main__':
    conf = get_spark_app_config()

    spark = SparkSession.builder\
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(1)

    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())
    survey_df = load_survey_df(spark, sys.argv[1])
    survey_df.show()

    spark.stop()



