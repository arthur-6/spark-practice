import sys
import datetime

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as f

from utils.logger import Log4J
from utils.utils import *

if __name__ == "__main__":

    conf = get_spark_app_config()

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    invoice_df = load_df_parquet(spark, 'output/part-00000-a4cc2bd1-a322-4fe7-b3d2-a5401a009af8-c000.snappy.parquet')

    running_total_window = Window.partitionBy('Country') \
        .orderBy('WeekNumber') \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    invoice_df.withColumn('RunningTotal', f.sum('InvoiceValue').over(running_total_window)).show()
