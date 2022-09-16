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

    NumInvoices = f.countDistinct('InvoiceNo').alias('NumInvoices')
    TotalQuantity = f.sum("Quantity").alias("TotalQuantity")
    InvoiceValue = f.round(f.sum(f.expr('Quantity * UnitPrice')), 2).alias('InvoiceValue')

    num_invoices_per_weekno_df = invoice_df \
        .withColumn('WeekNumber', weekofyear(expr('to_date(InvoiceDate, "dd-MM-yyyy H.mm")'))) \
        .groupBy('WeekNumber', 'Country') \
        .agg(NumInvoices, TotalQuantity, InvoiceValue) \
        .sort('Country', 'WeekNumber') \
    
    num_invoices_per_weekno_df.coalesce(1) \
        .write \
        .format('parquet') \
        .mode('overwrite') \
        .save('output')
    
