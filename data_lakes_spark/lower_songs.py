from pyspark import SparkSession

if __name__ == "__main__":

    """
        example program to show how to submit programs

    """

    spark = (
        SparkSession.builder.appName("LowerSongTitles").getOrCreate()
    )

    log_of_songs = [
        "Despacito",
        "Nice for what",
        "Now tears left to cry",
        "despacitO",
        "Havana",
        "In my feelings",
        "Nice for What",
        "despacito",
        "All of the stars"
    ]

    distributed_song_log = spark.sparkContext.parallelize(log_of_songs)

    print(
        distributed_song_log.map(lambda x: x.lower()).collect()
    )

    spark.stop()
