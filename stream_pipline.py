import os
import time
import datetime

from kafka.admin import KafkaAdminClient, NewTopic

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, round, to_json, from_json, struct, avg, lit

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
)

from configs import kafka_config, jdbc_config

TOPIC_EVENT_RESULTS_IX = 0
TOPIC_GROUP_RESULTS_IX = 1

topic_names = [
    f'{kafka_config['name']}_de_fp_athlete_event_results',
    f'{kafka_config['name']}_de_fp_group_results',
]

# Create Kafka topics
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'][0],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

num_partitions = 2
replication_factor = 1
new_topics = [ NewTopic(name=n, num_partitions=num_partitions, replication_factor=replication_factor) for n in topic_names ]

try:
    admin_client.create_topics(new_topics=new_topics, validate_only=False)
    print(f"Topics are created successfully.")
    [print(topic) for topic in admin_client.list_topics() if kafka_config['name'] in topic]
except Exception as e:
    print(f"An error occurred: {e}")

admin_client.close()

# Create Spark session
# Packet to work with Kafka in Spark
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

spark = SparkSession.builder \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .appName("DE_FP") \
    .getOrCreate()


def get_jdbc_df(jdbc_table, limit=None):
    df = spark.read.format('jdbc').options(
                url = f'{jdbc_config['url']}/olympic_dataset',
                driver = 'com.mysql.cj.jdbc.Driver',
                dbtable = jdbc_table,
                user = jdbc_config['user'],
                password = jdbc_config['password']) \
            .load()
    if limit:
        df = df.limit(limit)
    return df

# Read data athlete_bio
athlete_bio_raw_df = get_jdbc_df("athlete_bio")
athlete_bio_raw_df.show(10)

# Filter data
athlete_bio_df = athlete_bio_raw_df \
    .withColumn("height", col("height").cast(FloatType())) \
    .withColumn("weight", col("weight").cast(FloatType())) \
    .fillna({"height": 0, "weight": 0}) \
    .filter((col("height") != 0) & (col("weight") != 0))

athlete_bio_df.show(10)

# Reduce data before join
athlete_bio_short_df = athlete_bio_df.select(['athlete_id', 'sex', 'height', 'weight'])

# Read data athlete_event_results
athlete_event_results_raw_df = get_jdbc_df("athlete_event_results")
athlete_event_results_raw_df.show(5)

# Convert data to json
columns = athlete_event_results_raw_df.columns
athlete_event_results_df = athlete_event_results_raw_df.withColumn("key", expr("uuid()"))
athlete_event_results_json_df = athlete_event_results_df.select(
        'key',
        to_json(struct(columns)).alias('value')
    )
athlete_event_results_json_df.show(5)

# Write data to Kafka
athlete_event_results_json_df \
    .write \
        .format('kafka') \
        .option('kafka.bootstrap.servers', kafka_config['bootstrap_servers'][0]) \
        .option('topic', topic_names[TOPIC_EVENT_RESULTS_IX]) \
        .option('kafka.security.protocol', 'SASL_PLAINTEXT') \
        .option('kafka.sasl.mechanism', 'PLAIN') \
        .option('kafka.sasl.jaas.config',
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config['username']}" password="{kafka_config['password']}";') \
        .save()

# Read data from Kafka
json_schema = StructType([
        StructField('edition', StringType(), True),
        StructField('edition_id', IntegerType(), True),
        StructField('country_noc', StringType(), True),
        StructField('sport', StringType(), True),
        StructField('event', StringType(), True),
        StructField('result_id', StringType(), True),
        StructField('athlete', StringType(), True),
        StructField('athlete_id', IntegerType(), True),
        StructField('pos', StringType(), True),
        StructField('medal', StringType(), True),
        StructField('isTeamSport', StringType(), True),
])

df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', kafka_config['bootstrap_servers'][0]) \
        .option('kafka.security.protocol', 'SASL_PLAINTEXT') \
        .option('kafka.sasl.mechanism', 'PLAIN') \
        .option('kafka.sasl.jaas.config',
                f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config['username']}" password="{kafka_config['password']}";') \
        .option('subscribe', topic_names[TOPIC_EVENT_RESULTS_IX]) \
        .option('startingOffsets', 'earliest') \
        .option('maxOffsetsPerTrigger', '300') \
        .load()

clean_df = df.selectExpr('CAST(value AS STRING) AS value_deserialized') \
        .withColumn('value_json', from_json(col('value_deserialized'), json_schema)) \
        .withColumn('edition', col('value_json.edition')) \
        .withColumn('edition_id', col('value_json.edition_id')) \
        .withColumn('country_noc', col('value_json.country_noc')) \
        .withColumn('sport', col('value_json.sport')) \
        .withColumn('event', col('value_json.event')) \
        .withColumn('result_id', col('value_json.result_id')) \
        .withColumn('athlete', col('value_json.athlete')) \
        .withColumn('athlete_id', col('value_json.athlete_id')) \
        .withColumn('pos', col('value_json.pos')) \
        .withColumn('medal', col('value_json.medal')) \
        .withColumn('isTeamSport', col('value_json.isTeamSport')) \
        .drop('value_json', 'value_deserialized')

# Join data
joined_df = clean_df.join(athlete_bio_short_df, 'athlete_id', 'inner')

# Calculate average height and weight
grouped_df = joined_df \
    .groupBy('sport', 'medal', 'sex', 'country_noc') \
    .agg(
        round(avg('height'), 4).alias('avg_height'),
        round(avg('weight'), 4).alias('avg_weight'),
    ) \
    .withColumn('timestamp', lit(str(datetime.datetime.now())))


# Write data to Kafka
def foreach_batch_function(batch_df, batch_id):
    # convert to json
    columns = batch_df.columns
    batch_json_df = batch_df.withColumn("key", expr("uuid()"))
    batch_json_df = batch_json_df.select(
            'key',
            to_json(struct(columns)).alias('value')
        )

    # send to kafka
    batch_json_df.write \
        .format("kafka") \
        .option('kafka.bootstrap.servers', kafka_config['bootstrap_servers'][0]) \
        .option('topic', topic_names[TOPIC_GROUP_RESULTS_IX]) \
        .option('kafka.security.protocol', 'SASL_PLAINTEXT') \
        .option('kafka.sasl.mechanism', 'PLAIN') \
        .option('kafka.sasl.jaas.config',
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config['username']}" password="{kafka_config['password']}";') \
        .save()

    # send to mysql
    batch_df.write \
        .format("jdbc") \
        .option("url", f'{jdbc_config['url']}/{kafka_config['name']}') \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", 'athlete_enriched_agg') \
        .option("user", jdbc_config['user']) \
        .option("password", jdbc_config['password']) \
        .mode("append") \
        .save()


grouped_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .option('checkpointLocation', f'/tmp/checkpoints-{topic_names[TOPIC_GROUP_RESULTS_IX]}-display1') \
    .trigger(processingTime="10 seconds") \
    .start() \
    .awaitTermination()

spark.stop()
