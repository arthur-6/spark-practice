import sys

from pyspark.sql import *
from pyspark.sql.types import *

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

    flightSchemaStruct = StructType([
        StructField('FL_DATE', DateType()),
        StructField('OP_CARRIER', StringType()),
        StructField('OP_CARRIER_FL_NUM', IntegerType()),
        StructField('ORIGIN', StringType()),
        StructField('ORIGIN_CITY_NAME', StringType()),
        StructField('DEST', StringType()),
        StructField('DEST_CITY_NAME', StringType()),
        StructField('CRS_DEP_TIME', IntegerType()),
        StructField('DEP_TIME', IntegerType()),
        StructField('WHEELS_ON', IntegerType()),
        StructField('TAXI_IN', IntegerType()),
        StructField('CRS_ARR_TIME', IntegerType()),
        StructField('ARR_TIME', IntegerType()),
        StructField('CANCELLED', IntegerType()),
        StructField('DISTANCE', IntegerType()),
    ])

    # também pode ser usado DDL para definir o schema

    survey_df = load_survey_df_csv(spark, flightSchemaStruct, sys.argv[1])
    # survey_df = load_survey_df_json(spark, sys.argv[1])
    # survey_df = load_survey_df_parquet(spark, sys.argv[1])
    
    survey_df.show(5)

    spark.stop()