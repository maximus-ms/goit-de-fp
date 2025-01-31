import os
import requests
from pyspark.sql import SparkSession
from tempfile import NamedTemporaryFile
from pyspark import SparkConf


config = SparkConf() \
    .setMaster('spark://217.61.58.159:7077') \
    .setAppName('maksymp_de-fp_landing2bronze')


FTP = 'https://ftp.goit.study/neoversity/'

TABLES = [
    'athlete_bio',
    'athlete_event_results',
]
directory_out_path = 'bronze_maksymp'


def download_data(table_name, spark, dst_dir_path, ftp, output_format=None):
    downloading_url = ftp + table_name + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open the local file in write-binary mode and write the content of the response to it
        if output_format == "csv":
            with open(f'{dst_dir_path}/{table_name}.csv', 'wb') as file:
                file.write(response.content)
            print(f"File downloaded successfully and saved as {dst_dir_path}/{table_name}.csv")
        else:
            # Write the content of the response to a temporary file
            with NamedTemporaryFile(delete=True, mode='wb', suffix=".csv") as temp_csv:
                temp_csv.write(response.content)
                temp_csv.flush()
                df = spark.read.csv(temp_csv.name, header=True)
                df.show()

                path = f'{dst_dir_path}/{table_name}'
                df.write.parquet(path, mode="overwrite")
                print(f"File downloaded successfully and saved as parquet to {path}")

    else:
        raise Exception(f"Failed to download the file. Status code: {response.status_code}")


if not os.path.exists(directory_out_path):
    os.makedirs(directory_out_path)

spark = SparkSession.builder \
    .config(conf=config) \
    .getOrCreate()

for table in TABLES:
    download_data(table, spark, directory_out_path, FTP)
spark.stop()
