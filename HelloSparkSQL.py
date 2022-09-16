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
    
    survey_df.createOrReplaceTempView('table_survey')
    countDF = spark.sql('select Country, count(1) as count from table_survey where Age<40 group by Country')
    countDF.show()

    spark.stop()