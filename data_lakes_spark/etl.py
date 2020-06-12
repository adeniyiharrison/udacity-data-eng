import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StringType,
    TimestampType
)
from pyspark.sql.functions import (
    udf, monotonically_increasing_id
)

# # If running locally
# import glob
# import boto3
# from functools import reduce

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s : %(message)s"
    )


class RunETL():

    def __init__(
        self,
        local=True
    ):
        self.local = local
        self.sql_folder = "sql/"
        self.bucket_name = "adeniyi-data-lake-project"
        self.input_data = "s3a://adeniyi-data-lake-project/input-data/"
        self.output_data = "s3a://adeniyi-data-lake-project/output-data/"
        if local:
            os.environ['AWS_ACCESS_KEY_ID'] = os.getenv("UDACITY_KEY")
            os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv("UDACITY_SECRET")
            self.download_s3()
            self.spark = self.create_local_spark_session()
        elif not local:
            self.spark = (
                SparkSession
                .builder
                .appName("data_lake_project")
                .config(
                    "spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:2.7.0"
                )
                .getOrCreate()
            )

    def run(
        self
    ):
        logging.info("etl.py starting..")

        self.process_song_data()
        self.process_log_data()

        logging.info("etl.py completed..")

    def create_local_spark_session(
        self
    ):
        spark = SparkSession \
            .builder \
            .master("local") \
            .appName("data_lake_project") \
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:2.7.0"
            ) \
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
        logging.info("process_song_data starting..")

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

        elif not self.local:
            df = self.spark.read.json(
                self.input_data + "song-data/song_data/*/*/*/*"
            )

        # extract columns to create songs table
        df.createOrReplaceTempView("songs_data")
        songs_table = self.read_sql(
            table_name="songs_data",
            sql_file="create_songs_table.sql",
        )

        # write songs table to parquet files partitioned by year and artist
        logging.info("writing songs results")
        songs_table.write.mode("overwrite").partitionBy(
            "year", "artist_id"
        ).parquet(
            self.output_data +
            "songs.parquet_" +
            datetime.now().strftime("%Y-%m-%d")
        )

        # extract columns to create artists table
        artists_table = self.read_sql(
            table_name="songs_data",
            sql_file="create_artists_table.sql",
        )

        # write artists table to parquet files
        logging.info("writing artist results")
        artists_table.write.mode("overwrite").parquet(
            self.output_data +
            "artists.parquet_" +
            datetime.now().strftime("%Y-%m-%d")
        )

        logging.info("process_song_data completed..")

    def process_log_data(
        self
    ):

        logging.info("process_log_data starting..")

        @udf(TimestampType())
        def create_proper_ts(ts):
            return datetime.fromtimestamp(ts / 1000)

        @udf(StringType())
        def create_start_time(ts):
            return ts.strftime('%Y-%m-%d %H:%M:%S')

        if self.local:
            df = self.spark.read.json("data/input-data/log-data/*.json")
        elif not self.local:
            df = self.spark.read.json(self.input_data + "log-data/*.json")

        df = df.withColumn("ts", create_proper_ts("ts"))
        df = df.withColumn("startTime", create_start_time("ts"))

        # extract columns for users table
        df.createOrReplaceTempView("log_data")
        users_table = self.read_sql(
            table_name="log_data",
            sql_file="create_users_table.sql",
        )

        # write users table to parquet files
        users_table.write.mode("overwrite").parquet(
            self.output_data +
            "users.parquet_" +
            datetime.now().strftime("%Y-%m-%d")
        )

        # create timestamp column from original timestamp column
        df = df.filter(df.page == "NextSong")

        # extract columns to create time table
        df.createOrReplaceTempView("next_songs")
        times_table = self.read_sql(
                table_name="next_songs",
                sql_file="create_times_table.sql"
        )

        # write time table to parquet files partitioned by year and month
        times_table.write.mode("overwrite").parquet(
            self.output_data +
            "times.parquet_" +
            datetime.now().strftime("%Y-%m-%d")
        )

        # extract columns from joined song and log datasets
        # to create songplays table

        # ON ns.artist = s.artist_name
        songplays_table = self.spark.sql(
            """
                SELECT
                    ns.startTime AS start_time,
                    year(ns.ts) AS year,
                    month(ns.ts) AS month,
                    ns.userId AS user_id,
                    ns.level,
                    s.song_id,
                    s.artist_id,
                    ns.sessionId AS session_id,
                    ns.location,
                    userAgent AS user_agent
                FROM next_songs ns
                LEFT JOIN songs_data s
                    ON ns.song = s.title
                WHERE s.artist_id IS NOT NULL
                    AND s.title IS NOT NULL
            """
        )

        songplays_table = (
            songplays_table.withColumn(
                "songplay_id",
                monotonically_increasing_id()
            )
        )

        # write songplays table to parquet files
        songplays_table.select(
            [
                "songplay_id",
                "start_time",
                "user_id",
                "level",
                "song_id",
                "artist_id",
                "session_id",
                "location",
                "user_agent"
            ]
        ).write.mode("overwrite").parquet(
            self.output_data +
            "songplays.parquet_" +
            datetime.now().strftime("%Y-%m-%d")
        )

        logging.info("process_log_data completed..")


if __name__ == "__main__":
    etl = RunETL(
        local=False
    )
    etl.run()
