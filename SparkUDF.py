import sys
import re

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

from utils.logger import Log4J
from utils.utils import *

def parse_gender(gender_clmn):
    female_pattern = r'^f$|f.m|w.m'
    male_pattern = r'^m$|ma|m.l'

    if re.search(female_pattern, gender_clmn.lower()):
        return "Female"
    elif re.search(male_pattern, gender_clmn.lower()):
        return "Male"
    else:
        return "Unknown"

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

    survey_df = load_df_csv(spark, sys.argv[1])
    survey_df.show()

    parse_gender_udf = udf(parse_gender, StringType())
    logger.info('Catalog Entry:')
    [logger.info(f) for f in spark.catalog.listFunctions() if 'parse_gender' in f.name]
    survey_df2 =  survey_df.withColumn('Gender', parse_gender_udf('Gender'))
    survey_df2.show()
    
    spark.udf.register('parse_gender_udf', parse_gender, StringType())
    logger.info('Catalog Entry:')
    [logger.info(f) for f in spark.catalog.listFunctions() if 'parse_gender' in f.name]
    survey_df3 =  survey_df.withColumn('Gender', expr('parse_gender_udf(Gender)'))
    survey_df3.show()