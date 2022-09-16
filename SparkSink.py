import sys

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import spark_partition_id

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

    survey_df = load_survey_df_parquet(spark, sys.argv[1])

    logger.info('Num partitions before: ' + str(survey_df.rdd.getNumPartitions()))
    survey_df.groupBy(spark_partition_id()).count().show()

    partitioned_df = survey_df.repartition(5)
    logger.info('Num partitions after: ' + str(partitioned_df.rdd.getNumPartitions()))
    partitioned_df.groupBy(spark_partition_id()).count().show()

    # partitioned_df.write \
    #     .format('json') \
    #     .mode('overwrite') \
    #     .option('path', 'dataWrites/json/') \
    #     .save()

    survey_df.write \
        .format('json') \
        .mode('overwrite') \
        .option('path', 'dataWrites/json/') \
        .option('maxRecordsPerFile', 10000) \
        .partitionBy('OP_CARRIER', 'ORIGIN') \
        .save()

    spark.stop()