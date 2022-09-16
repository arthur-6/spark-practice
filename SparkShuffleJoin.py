from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as f

from utils.utils import *

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('Shuffle Join') \
        .master('local[3]') \
        .getOrCreate()

    flight_time_df1 = load_df_json(spark, 'data/d1/')
    flight_time_df2 = load_df_json(spark, 'data/d2/')

    spark.conf.set('spark.sql.shuffle.partitions', 3)

    join_expr = flight_time_df1.id == flight_time_df2.id
    # join_df = flight_time_df1.join(flight_time_df2, join_expr, 'inner')
    join_df = flight_time_df1.join(broadcast(flight_time_df2), join_expr, 'inner')


    join_df.collect()

    input('press to stop')