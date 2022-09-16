import sys

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

from utils.logger import Log4J
from utils.utils import *


if __name__ == "__main__":

    conf = get_spark_app_config()

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error('Usage: HelloSpark <filename>')
        sys.exit(-1)

    logger.info('Starting HelloSpark')

    flighttime_df = load_survey_df_parquet(spark, sys.argv[1])

    # flighttime_df.select('OP_CARRIER', 'ORIGIN').show(10)
    # flighttime_df.select(column('ORIGIN'), col('OP_CARRIER'), flighttime_df.DEST, 'DISTANCE').show(10)
    # flighttime_df.select(expr('concat(ORIGIN, " - ", DEST) as ITINERARY'), 'DISTANCE').show(10)
    flighttime_df.select(concat("ORIGIN", lit(' - '), "DEST").alias("ITINERARY"), "DISTANCE").show(10)