# Project 1: Data modeling with Postgres

### Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

### Project Description

In this project, you'll apply what you've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, you will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

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

___Songplays___
| Column Name | Data Type                                        |
|-------------|--------------------------------------------------|
| Songplay_id | int |
| start_time  | timestamp                                        |
| user_id     | int                                              |
| level       | varchar                                          |
| song_id     | varchar                                          |
| artist_id   | varchar                                          |
| session_id  | int                                              |
| location    | varchar                                          |
| user_agent  | varchar                                          |

___users___
| Column Name | Data Type |
|-------------|-----------|
| user_id     | int       |
| first_name  | varchar   |
| last_name   | varchar   |
| gender      | varchar   |
| level       | varchar   |

___songs___
| Column Name | Data Type |
|-------------|-----------|
| song_id     | varchar   |
| title       | varchar   |
| artist_id   | varchar   |
| year        | int       |
| duration    | float     |

___artists___
| Column Name | Data Type |
|-------------|-----------|
| artist_id   | varchar   |
| name        | varchar   |
| location    | varchar   |
| latitude    | float     |
| longitude   | float     |

___timestamps___
| Column Name | Data Type |
|-------------|-----------|
| start_time  | timestamp |
| hour        | int       |
| day         | int       |
| week        | int       |
| month       | int       |
| year        | int       |
| weekday     | int       |


### Special Cases
* When duplicates found in `users`, the table will keep the lastest `level` value and the existing value will be replaced.
* Potential duplicates to be added into the other tables will be ignored.

### Running Job/Script
1. Call `python create_tables.py` in the terminal to prep tables
2. Then ultimately `python etl.py` to populate tables