import os
import glob
import psycopg2
import pandas as pd
from sql_queries import (
    song_select,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
    user_table_insert,
    songplay_table_insert
)


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(
        filepath,
        typ="series"
    )

    for col in ["artist_name", "title"]:
        df[col] = df[col].replace("'", "")

    # insert song record
    cur.execute(
        song_table_insert,
        df[[
            "song_id",
            "title",
            "artist_id",
            "year",
            "duration"
        ]]
    )

    # insert artist record
    cur.execute(
        artist_table_insert,
        df[[
            "artist_id",
            "artist_name",
            "artist_location",
            "artist_latitude",
            "artist_longitude"
        ]]
    )


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(
        filepath,
        lines=True
    )
    df = df.loc[df.page == "NextSong"]
    df.replace("", None, inplace=True)
    df["ts"] = pd.to_datetime(df["ts"])
    df["userId"] = df["userId"].astype(int)

    for col in ["artist", "song"]:
        df[col] = df[col].apply(
            lambda x: x.replace("'", "") if x else None
        )

    # insert time data records
    time_df = pd.DataFrame()
    time_df["start_time"] = df["ts"].copy()
    time_df["hour"] = time_df["start_time"].apply(lambda x: x.hour)
    time_df["day"] = time_df["start_time"].apply(lambda x: x.day)
    time_df["week"] = time_df["start_time"].apply(lambda x: x.week)
    time_df["month"] = time_df["start_time"].apply(lambda x: x.month)
    time_df["year"] = time_df["start_time"].apply(lambda x: x.year)
    time_df["weekday"] = time_df["start_time"].apply(lambda x: x.weekday())

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.loc[
        (df.userId.notnull()),
        [
            "userId",
            "firstName",
            "lastName",
            "gender",
            "level"
        ]
    ].drop_duplicates(subset=["userId"])

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [
            row.ts,
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent
        ]

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    conn.autocommit = True
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
