import os
import glob
import boto3
import logging
from functools import reduce
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import (
    year, month, dayofmonth, hour, weekofyear, date_format
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s : %(message)s"
    )

os.environ['AWS_ACCESS_KEY_ID'] = os.getenv("UDACITY_KEY")
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv("UDACITY_SECRET")


class RunETL():

    def __init__(
        self,
        local=True
    ):
        self.local = local
        self.sql_folder = "sql/"
        self.bucket_name = "adeniyi-data-lake-project"
        self.input_data = "s3://adeniyi-data-lake-project/input-data/"
        self.output_data = "s3a://adeniyi-data-lake-project/output-data/"
        if local:
            self.download_s3()
            self.spark = self.create_local_spark_session()

    def run(
        self
    ):

        self.process_song_data()
        self.process_log_data()

        breakpoint()

        logging.info("done")

    def create_local_spark_session(
        self
    ):
        spark = SparkSession \
            .builder \
            .master("local") \
            .appName("data_lake_project") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()

        logging.info("create_local_spark_session completed")
        return spark

    def download_s3(
        self
    ):
        s3 = boto3.resource(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )

        bucket = s3.Bucket(self.bucket_name)

        for file_object in bucket.objects.all():
            if str(file_object.key).endswith(".json"):
                os.makedirs(
                    os.path.dirname("data/" + file_object.key),
                    exist_ok=True
                )
            else:
                continue
            bucket.download_file(
                file_object.key, "data/" + file_object.key
            )

        logging.info("download_s3 completed")

    def read_sql(
        self,
        table_name,
        sql_file,
    ):

        with open(self.sql_folder + sql_file, 'r') as f:
            query = f.read().format(
                table_name=table_name
            )

        return self.spark.sql(query)

    def process_song_data(
        self
    ):

        if self.local:
            # Running locally with test files
            def _listdir_fullpath(d):
                return [os.path.join(d, f) for f in os.listdir(d)]

            # read song data
            song_data_dir = (
                [
                    f for f in glob.glob(
                        "data/input-data/song-data/" + "**/",
                        recursive=True
                    )
                ]
            )
            song_data_files = [
                y for x in song_data_dir for y in _listdir_fullpath(x)
            ]
            song_data_json = [
                x for x in song_data_files if x.endswith(".json")
            ]

            dfAppend = []
            for json_file in song_data_json:
                df = self.spark.read.json(json_file)
                dfAppend.append(df)

            df = reduce(DataFrame.unionAll, dfAppend)

            # extract columns to create songs table
            df.createOrReplaceTempView("log_data")
            songs_table = self.read_sql(
                table_name="log_data",
                sql_file="create_songs_table.sql",
            )

            # write songs table to parquet files partitioned by year and artist
            logging.info("writing songs results")
            songs_table.write.partitionBy(
                "year", "artist_id"
            ).parquet(
                self.output_data +
                "songs.parquet_" +
                datetime.now().strftime("%Y-%m-%d-%H-%M")
            )

            # # extract columns to create artists table
            artists_table = self.read_sql(
                table_name="log_data",
                sql_file="create_artists_table.sql",
            )

            # write artists table to parquet files
            logging.info("writing artist results")
            artists_table.write.parquet(
                self.output_data +
                "artists.parquet_" +
                datetime.now().strftime("%Y-%m-%d-%H-%M")
            )
        else:
            # READ FROM INPUT SOURCE DIRECTLY FROM EMR CLUSTER
            df = self.spark.read.json(self.input_data + "/log-data")

    def process_log_data(
        self
    ):
        pass
        # # get filepath to log data file
        # log_data =

        # # read log data file
        # df = 
        
        # # filter by actions for song plays
        # df = 

        # # extract columns for users table    
        # artists_table = 
        
        # # write users table to parquet files
        # artists_table

        # # create timestamp column from original timestamp column
        # get_timestamp = udf()
        # df = 
        
        # # create datetime column from original timestamp column
        # get_datetime = udf()
        # df = 
        
        # # extract columns to create time table
        # time_table = 
        
        # # write time table to parquet files partitioned by year and month
        # time_table

        # # read in song data to use for songplays table
        # song_df = 

        # # extract columns from joined song and log datasets to create songplays table 
        # songplays_table = 

        # # write songplays table to parquet files partitioned by year and month
        # songplays_table


if __name__ == "__main__":
    etl = RunETL()
    etl.run()
