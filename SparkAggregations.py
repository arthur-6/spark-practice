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

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error('Usage: HelloSpark <filename>')
        sys.exit(-1)

    logger.info('Starting HelloSpark')

    invoice_df = load_df_csv(spark, sys.argv[1])

    #Simple aggregations
    invoice_df.select(
        f.count("*").alias("Count *"),
        f.sum('Quantity').alias('TotalQuantity'),
        f.avg('UnitPrice').alias('AvgPrice'),
        f.countDistinct('InvoiceNo').alias('CountDistinct')).show()

    invoice_df.selectExpr(
        "count(1) as `Count 1`",
        "count(StockCode) as `Count Field`", 
        "sum(Quantity) as `TotalQuantity`", 
        "avg(UnitPrice) as `AvgPrice`", 
        'count(distinct InvoiceNo) as `CountDistinct`').show()
    
    #Grouping aggregations
    invoice_df.createOrReplaceTempView('sales')
    summary_sql = spark.sql("""
    select Country, InvoiceNo,
        sum(Quantity) as TotalQuantity,
        round(sum(Quantity * UnitPrice), 2) as InvoiceValue
    from sales
    group by Country, InvoiceNo
    """)
    summary_sql.show()

    summary_df = invoice_df \
        .groupBy('Country', 'InvoiceNo') \
        .agg(f.sum("Quantity").alias("TotalQuantity"),
        f.round(f.sum(f.expr('Quantity * UnitPrice')), 2).alias('InvoiceValue'),
        f.expr('round(sum(Quantity * UnitPrice), 2) as InvoiceValueExpr')) \
        .show()