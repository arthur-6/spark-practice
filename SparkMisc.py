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

   #Quick dataframe creation
    data_list = [
        ("Arthur", "29", "6", "00"),
        ("Clarice", "28", "6", "04"), 
        ("Heitor", "1", "11", "08"),
        ("Rafael", "12", "4", "2000"),
        ("Arthur", "29", "6", "00")
    ]
    raw_df = spark.createDataFrame(data_list).toDF('name', 'day', 'month', 'year').repartition(3)
    raw_df.printSchema()

    #ID creation
    df1 = raw_df.withColumn('id', monotonically_increasing_id())
    df1.show()

    #Case when then
    df2 = df1.withColumn('year', expr("""
        case when year < 23 then year + 2000
        when year < 100 then year + 1900
        else year
        end""")) # will not work, year is a string field
    df2.show() 

    #Cast dataframe columns
    ##Inline cast
    df3 = df1.withColumn('year', expr("""
        case when cast(year as int) < 23 then cast(year as int) + 2000
        when cast(year as int) < 100 then cast(year as int) + 1900
        else year
        end""")) 
    df3.show() 

    ##Changing schema
    df4 = df1.withColumn('year', expr("""
        case when year < 23 then year + 2000
        when year < 100 then year + 1900
        else year
        end""").cast(IntegerType()))
    df4.show() 
    df4.printSchema()

    df5 = df1.withColumn('day', col('day').cast(IntegerType())) \
    .withColumn('month', col('month').cast(IntegerType())) \
    .withColumn('year', col('year').cast(IntegerType()))

    df6 = df5.withColumn('year', expr("""
        case when year < 23 then year + 2000
        when year < 100 then year + 1900
        else year
        end"""))
    df6.show()
    df6.printSchema()

    df7 = df5.withColumn('year', when(col('year') < 21, col('year') + 2000) \
                                .when(col('year') < 100, col('year') + 1900) \
                                .otherwise(col('year')))
    df7.show()

    #Creating and droping columns
    df8 = df7.withColumn('dob', expr("to_date(concat(day,'/',month,'/',year), 'd/M/y')"))
    df8.show()

    df9 = df7.withColumn('dob', to_date(expr("concat(day,'/',month,'/',year)"), 'd/M/y')) \
        .drop('day', 'month', 'year') \
        .dropDuplicates(['name', 'dob']) \
        .sort(expr('dob desc'))
    df9.show()