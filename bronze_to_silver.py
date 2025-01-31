import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark import SparkConf

config = SparkConf() \
    .setMaster('spark://217.61.58.159:7077') \
    .setAppName("maksymp_de-fp_bronze2silver")


TABLES = [
    'athlete_bio',
    'athlete_event_results',
]
directory_in_path = 'bronze_maksymp'
directory_out_path = 'silver_maksymp'

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

def process_data(table_name, spark):
    df = spark.read.parquet(f'{directory_in_path}/{table_name}')
    clean_text_udf = udf(clean_text, StringType())

    for col_name, col_type in df.dtypes:
        if col_type == 'string':
            df = df.withColumn(col_name, clean_text_udf(df[col_name]))

    df = df.dropDuplicates()
    df.show()

    df.write.parquet(f'{directory_out_path}/{table_name}', mode='overwrite')
    print(f"File downloaded successfully and saved as parquet to {directory_out_path}/{table_name}")

if not os.path.exists(directory_in_path):
    raise Exception(f"Directory {directory_in_path} does not exist")

if not os.path.exists(directory_out_path):
    os.makedirs(directory_out_path)


spark = SparkSession.builder \
    .config(conf=config) \
    .getOrCreate()

for table in TABLES:
    process_data(table, spark)

spark.stop()
