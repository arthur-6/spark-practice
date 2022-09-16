import sys

from pyspark.sql import *
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

    survey_df = load_survey_df_csv(spark, sys.argv[1])
    
    partitioned_df = survey_df.repartition(2)
    count_df = count_by_country(partitioned_df)

    logger.info(count_df.collect())

    input('press enter')
    logger.info('Finished HelloSpark')

    spark.stop()