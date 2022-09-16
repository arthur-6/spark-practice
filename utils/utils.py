import configparser
import sys

from pyspark import SparkConf

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read('spark.conf')

    for (key, val) in config.items('SPARK_APP_CONFIGS'):
        spark_conf.set(key, val)
    return spark_conf

def load_df_csv(spark, data_file):
    return spark.read \
        .format('csv') \
        .option('header', 'true') \
        .option('mode', 'FAILFAST') \
        .option('dateFormat', 'M/d/y') \
        .load(data_file) 

def load_df_json(spark, data_file):
    return spark.read \
        .format('json') \
        .load(data_file)

def load_df_parquet(spark, data_file):
    return spark.read \
        .format('parquet') \
        .load(data_file)

def count_by_country(df):
    return df.where('Age < 40') \
        .select('Age', 'Gender', 'Country', 'state') \
        .groupBy('Country') \
        .count()