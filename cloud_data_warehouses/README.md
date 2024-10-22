# Project 3: Data Warehousing

### Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

### Project Description

Using the song and event datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the tables found in the Schema section below. 

1. Create Redshift cluster using `create_cluster.py`
2. Define you SQL statements, which will be imported into the `create_table.py` and `etl.py` files.
3. Create your fact and dimension tables for the star schema in Redshift using `create_tables.py`
4. Load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift using `etl.py`
 

### Song Dataset

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

`song_data/A/B/C/TRABCEI128F424C983.json`
`song_data/A/A/B/TRAABJL12903CDCF1A.json`

And below is an example of what a single song file,     `TRAABJL12903CDCF1A.json`, looks like.

```
{
    "num_songs": 1,
    "artist_id": "ARJIE2Y1187B994AB7", 
    "artist_latitude": null,
    "artist_longitude": null,
    "artist_location": "",
    "artist_name": "Line Renaud",
    "song_id": "SOUPIRU12A6D4FA1E1",
    "title": "Der Kleine Dompfaff",
    "duration": 152.92036,
    "year": 0
}
```

### Log Dataset

The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

`log_data/2018/11/2018-11-12-events.json`
`log_data/2018/11/2018-11-13-events.json`

And below is an example of what the data in a log file, 2018-11-12-events.json, looks like..

![log-data](img/log-data.png)

### Schema

![sparkify_schema](img/sparkify_star_schema.png)


### Running Job/Script
1. Set `UDACITY_KEY` and `UDACITY_SECRET` variables in environment
2. Call `python create_cluster.py` in the terminal to prep cluster
3. Call `python create_tables.py` in the terminal to prep tables
4. Then ultimately `python etl.py` to populate tables