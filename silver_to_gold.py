import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import round, avg, lit
from pyspark import SparkConf

config = SparkConf() \
    .setMaster('spark://217.61.58.159:7077') \
    .setAppName("maksymp_de-fp_silver2gold")


athlete_bio_table = 'athlete_bio'
athlete_event_results_table = 'athlete_event_results'
avg_stats_table = 'avg_stats'

directory_in_path = 'silver_maksymp'
directory_out_path = 'gold_maksymp'


if not os.path.exists(directory_in_path):
    raise Exception(f"Directory {directory_in_path} does not exist")

if not os.path.exists(directory_out_path):
    os.makedirs(directory_out_path)

spark = SparkSession.builder \
    .config(conf=config) \
    .getOrCreate()

athlete_bio_df = spark.read \
    .parquet(f'{directory_in_path}/{athlete_bio_table}')
athlete_event_results_df = spark.read \
    .parquet(f'{directory_in_path}/{athlete_event_results_table}')

print(athlete_bio_table)
athlete_bio_df.show()
print(athlete_event_results_table)
athlete_event_results_df.show()

athlete_bio_short_df = athlete_bio_df.select(['athlete_id', 'sex', 'height', 'weight'])

joined_df = athlete_event_results_df.join(athlete_bio_short_df, "athlete_id", "inner")

avg_stats_df = joined_df \
    .groupBy('sport', 'medal', 'sex', 'country_noc') \
    .agg(
        round(avg('height'), 4).alias('avg_height'),
        round(avg('weight'), 4).alias('avg_weight'),
    ) \
    .withColumn('timestamp', lit(str(datetime.datetime.now())))

avg_stats_df.show()

avg_stats_df.write \
    .parquet(f'{directory_out_path}/{avg_stats_table}', mode='overwrite')
print(f"File downloaded successfully and saved as parquet to {directory_out_path}/{avg_stats_table}")

spark.stop()
