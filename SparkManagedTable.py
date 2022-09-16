import sys

from pyspark.sql import *
from pyspark.sql.types import *

from utils.logger import Log4J
from utils.utils import *


if __name__ == "__main__":

    conf = get_spark_app_config()

    spark = SparkSession.builder \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error('Usage: HelloSpark <filename>')
        sys.exit(-1)

    logger.info('Starting HelloSpark')

    survey_df = load_survey_df_parquet(spark, sys.argv[1])

    spark.sql('CREATE DATABASE IF NOT EXISTS AIRLINE_DB')    
    spark.catalog.setCurrentDatabase('AIRLINE_DB')

    survey_df.write \
        .format('json') \
        .mode('overwrite') \
        .bucketBy(5, 'OP_CARRIER', 'ORIGIN') \
            .sortBy('OP_CARRIER', 'ORIGIN') \
        .saveAsTable('table_flight_data')
    
    logger.info(spark.catalog.listTables('AIRLINE_DB'))