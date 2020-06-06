import os
import glob
import fnmatch
import zipfile
from functools import reduce
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import (
    year, month, dayofmonth, hour, weekofyear, date_format
)


os.environ['AWS_ACCESS_KEY_ID'] = os.getenv("UDACITY_KEY")
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv("UDACITY_SECRET")


def create_spark_session():
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("data_lake_project") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_zips(
    run=False
):
    if run:
        for root, dirs, files in os.walk(os.getcwd()):
            for filename in fnmatch.filter(files, "*.zip"):
                print(os.path.join(root, filename))
                zipfile.ZipFile(
                    os.path.join(root, filename)
                ).extractall(
                    os.path.join(root, os.path.splitext(filename)[0])
                )


def read_sql(
    spark_object,
    table_name,
    sql_file,
    sql_folder="sql/"
):

    with open(sql_folder + sql_file, 'r') as f:
        query = f.read().format(
            table_name=table_name
        )

    return spark_object.sql(query)


def process_song_data(spark, input_data, output_data, local=True):

    if local:
        # Running locally with test files
        def _listdir_fullpath(d):
            return [os.path.join(d, f) for f in os.listdir(d)]

        # read song data
        song_data_dir = (
            [f for f in glob.glob("data/song-data/" + "**/", recursive=True)]
        )
        song_data_files = [
            y for x in song_data_dir for y in _listdir_fullpath(x)
        ]
        song_data_json = [
            x for x in song_data_files if x.endswith(".json")
        ]

        dfAppend = []
        for json_file in song_data_json:
            df = spark.read.json(json_file)
            dfAppend.append(df)

        df = reduce(DataFrame.unionAll, dfAppend)
    else:
        # FIGURE OUT HOW TO READ FROM INPUT SOURCE DIRECTLY FROM EMR CLUSTER
        # TRANSFER THIS TO EMR WHEN DONE PLAYING LOCALLY
        df = spark.read.json(input_data + "/log-data")

    # extract columns to create songs table
    df.createOrReplaceTempView("log_data")
    songs_table = read_sql(
        spark_object=spark,
        table_name="log_data",
        sql_file="create_songs_table.sql",
    )

    # write songs table to parquet files partitioned by year and artist
    print("writing songs results")
    songs_table.write.partitionBy(
        "year", "artist_id"
    ).parquet(
        "data_lakes_spark/data/output-data/" +
        "songs.parquet_" +
        datetime.now().strftime("%Y-%m-%d-%H-%M")
    )

    # # extract columns to create artists table
    artists_table = read_sql(
        spark_object=spark,
        table_name="log_data",
        sql_file="create_artists_table.sql",
    )

    # write artists table to parquet files
    print("writing artist results")
    artists_table.write.parquet(
        "data_lakes_spark/data/output-data/" +
        "artists.parquet" +
        datetime.now().strftime("%Y-%m-%d-%H-%M")
    )


# def process_log_data(spark, input_data, output_data):
#     # get filepath to log data file
#     log_data =

#     # read log data file
#     df = 
    
#     # filter by actions for song plays
#     df = 

#     # extract columns for users table    
#     artists_table = 
    
#     # write users table to parquet files
#     artists_table

#     # create timestamp column from original timestamp column
#     get_timestamp = udf()
#     df = 
    
#     # create datetime column from original timestamp column
#     get_datetime = udf()
#     df = 
    
#     # extract columns to create time table
#     time_table = 
    
#     # write time table to parquet files partitioned by year and month
#     time_table

#     # read in song data to use for songplays table
#     song_df = 

#     # extract columns from joined song and log datasets to create songplays table 
#     songplays_table = 

#     # write songplays table to parquet files partitioned by year and month
#     songplays_table


def main():
    # set run parameter to True if the jsons have yet to be unzipped
    process_zips()
    spark = create_spark_session()
    input_data = "s3://adeniyi-data-lake-project/input-data"
    output_data = "s3://adeniyi-data-lake-project/output-data"

    process_song_data(spark, input_data, output_data)
    # process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
