from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as f

from utils.utils import *

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('Bucket Join') \
        .master('local[3]') \
        .enableHiveSupport() \
        .getOrCreate()

    flight_time_df1 = spark.read.json('data/d1/')
    flight_time_df2 = spark.read.json('data/d2/')

    # spark.conf.set('spark.sql.shuffle.partitions', 3)

    # spark.sql('CREATE DATABASE IF NOT EXISTS MY_DB')
    # spark.sql('USE MY_DB')

    # flight_time_df1.coalesce(1).write \
    #     .bucketBy(3, 'id') \
    #     .mode('overwrite') \
    #     .saveAsTable('MY_DB.flight_data1')

    # flight_time_df2.coalesce(1).write \
    #     .bucketBy(3, 'id') \
    #     .mode('overwrite') \
    #     .saveAsTable('MY_DB.flight_data2')

    flight_time_df3 = spark.read.table('MY_DB.flight_data1')
    flight_time_df4 = spark.read.table('MY_DB.flight_data2')

    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', -1)

    join_expr = flight_time_df3.id == flight_time_df4.id
    join_df = flight_time_df3.join(flight_time_df4, join_expr, 'inner')

    join_df.collect()
    input('press to stop')