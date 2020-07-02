import boto3
import logging
from io import StringIO
import spotipy
from spotipy import oauth2
from pandas import DataFrame
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s : %(message)s"
    )


def return_spotipy_client():
    ccm = oauth2.SpotifyClientCredentials(
        client_id=Variable.get("spotify_client_id"),
        client_secret=Variable.get("spotify_client_secret")
    )

    spc = spotipy.Spotify(
        client_credentials_manager=ccm
    )

    return spc


def return_s3_client():

    s3 = boto3.client(
        "s3",
        region_name="us-west-1",
        aws_access_key_id=Variable.get("aws_key"),
        aws_secret_access_key=Variable.get("aws_secret")
    )

    return s3


def stage_streams(
    **kwargs
):
    connection_id = kwargs["redshift_conn_id"]
    redshift = PostgresHook(connection_id)
    key = Variable.get("aws_key")
    secret = Variable.get("aws_secret")

    table_name = kwargs["table_name"]
    s3_bucket = kwargs["s3_bucket"]
    s3_key = kwargs["s3_key"]
    s3_path = "s3://{}/{}".format(s3_bucket, s3_key)

    total_records = redshift.get_records(
        f"""
            SELECT
                COUNT(1) AS total_records
            FROM {table_name}

        """
    )
    total_records = total_records[0][0]

    if total_records == 0:
        redshift.run(
            f"""
                COPY {table_name}
                FROM '{s3_path}'
                ACCESS_KEY_ID '{key}'
                SECRET_ACCESS_KEY '{secret}'
                REGION AS 'us-west-1'
                IGNOREHEADER 1
                CSV;
            """
        )


def dataframe_to_s3(
    s3_client,
    dataframe,
    key
):

    csv_buffer = StringIO()
    dataframe.to_csv(
        csv_buffer,
        index=False,
        header=False
    )

    s3_client.put_object(
        Body=csv_buffer.getvalue(),
        Bucket="adeniyi-capstone-project",
        Key=key
    )


def enrich_streams_data(
    **kwargs
):
    """
        Hit Spotify API via Spotipy library
    """

    date_str = kwargs["execution_date"].strftime("%Y-%m-%d")
    connection_id = kwargs["redshift_conn_id"]
    redshift = PostgresHook(connection_id)
    spc = return_spotipy_client()
    s3 = return_s3_client()

    daily_streams = redshift.get_records(
        f"""
            SELECT
                track_name,
                artist AS artist_name,
                url AS track_url
            FROM streams_staging
            WHERE date = '{date_str}'
        """
    )

    track_results = []
    for track_name, artist_name, track_url in daily_streams:
        song_metadata = spc.search(
            q=f"artist:{artist_name} track:{track_name}",
            type="track",
            limit=1
        )

        track_dict = {}
        try:
            song_metadata = song_metadata["tracks"]["items"][0]
            track_dict["track_name"] = track_name
            track_dict["artist_name"] = artist_name
            track_dict["track_url"] = track_url
            track_dict["album_name"] = song_metadata["album"]["name"]
            track_dict["album_type"] = song_metadata["album"]["album_type"]
            track_dict["album_release_date"] = (
                song_metadata["album"]["release_date"]
            )
            track_dict["popularity"] = song_metadata["popularity"]
            track_dict["duration"] = song_metadata["duration_ms"]
            track_dict["explicit"] = song_metadata["explicit"]

            track_results.append(track_dict)

        except Exception as e:
            logging.info(e)
            pass

    track_results = DataFrame(track_results).drop_duplicates()
    dataframe_to_s3(
        s3_client=s3,
        dataframe=track_results,
        key=f"track_metadata/track_metadata_{date_str}.csv"
    )


def s3_to_redshift(
    **kwargs
):
    connection_id = kwargs["redshift_conn_id"]
    redshift = PostgresHook(connection_id)
    key = Variable.get("aws_key")
    secret = Variable.get("aws_secret")

    date = kwargs["execution_date"].strftime("%Y-%m-%d")
    table_name = kwargs["table_name"]
    s3_bucket = kwargs["s3_bucket"]
    s3_key = kwargs["s3_key"]
    s3_path = f"s3://{s3_bucket}/{s3_key}_{date}"

    redshift.run(
        f"""
            CREATE TEMP TABLE {table_name}_temp (LIKE {table_name});

            COPY {table_name}_temp
            FROM '{s3_path}'
            ACCESS_KEY_ID '{key}'
            SECRET_ACCESS_KEY '{secret}'
            REGION AS 'us-west-1'
            DELIMITER ','
            CSV;

            DELETE FROM {table_name}
            USING {table_name}_temp
            WHERE {table_name}.track_url = {table_name}_temp.track_url;

            INSERT INTO {table_name}
            SELECT *
            FROM {table_name}_temp;
        """
    )


def upsert_tracks(
    **kwargs
):
    connection_id = kwargs["redshift_conn_id"]
    redshift = PostgresHook(connection_id)
    date = kwargs["execution_date"].strftime("%Y-%m-%d")

    redshift.run(
        f"""
            CREATE TEMP TABLE tracks_temp (LIKE tracks);

            INSERT INTO tracks_temp (
                track_url,
                track_name,
                duration,
                popularity,
                explicit
                )
            SELECT
                t.track_url,
                t.track_name,
                t.duration,
                t.popularity,
                t.explicit
            FROM tracks_metadata t
            LEFT JOIN streams_staging s
                ON s.url = t.track_url
            WHERE s.date = '{date}';

            DELETE FROM tracks
            USING tracks_temp
            WHERE tracks.track_url = tracks_temp.track_url;

            INSERT INTO tracks (
                track_url,
                track_name,
                duration,
                popularity,
                explicit
                )
            SELECT
                track_url,
                track_name,
                duration,
                popularity,
                explicit
            FROM tracks_temp;
        """
    )


def upsert_albums(
    **kwargs
):
    connection_id = kwargs["redshift_conn_id"]
    redshift = PostgresHook(connection_id)
    date = kwargs["execution_date"].strftime("%Y-%m-%d")

    redshift.run(
        f"""
            CREATE TEMP TABLE albums_temp (LIKE albums);

            INSERT INTO albums_temp (
                album_name,
                album_type,
                release_date
                )
            SELECT DISTINCT
                t.album_name,
                t.album_type,
                t.album_release_date
            FROM tracks_metadata t
            LEFT JOIN streams_staging s
                ON s.url = t.track_url
            WHERE s.date = '{date}';

            DELETE FROM albums
            USING albums_temp
            WHERE albums.album_name = albums_temp.album_name
                AND albums.album_type = albums_temp.album_type
                AND albums.release_date = albums.release_date;

            INSERT INTO albums (
                album_name,
                album_type,
                release_date
                )
            SELECT
                album_name,
                album_type,
                release_date
            FROM albums_temp;
        """
    )


def upsert_regions(
    **kwargs
):
    connection_id = kwargs["redshift_conn_id"]
    redshift = PostgresHook(connection_id)
    date = kwargs["execution_date"].strftime("%Y-%m-%d")

    redshift.run(
        f"""
            CREATE TEMP TABLE regions_temp (LIKE regions);

            INSERT INTO regions_temp (
                region_name
                )
            SELECT DISTINCT
                region
            FROM streams_staging
            WHERE date = '{date}';

            DELETE FROM regions
            USING regions_temp
            WHERE regions.region_name = regions_temp.region_name;

            INSERT INTO regions (
                region_name
                )
            SELECT
                region_name
            FROM regions_temp;
        """
    )


def upsert_artists(
    **kwargs
):
    connection_id = kwargs["redshift_conn_id"]
    redshift = PostgresHook(connection_id)
    date = kwargs["execution_date"].strftime("%Y-%m-%d")

    redshift.run(
        f"""
            CREATE TEMP TABLE artists_temp (LIKE artists);

            INSERT INTO artists_temp (
                artist_name
                )
            SELECT DISTINCT
                artist
            FROM streams_staging
            WHERE date = '{date}';

            DELETE FROM artists
            USING artists_temp
            WHERE artists.artist_name = artists_temp.artist_name;

            INSERT INTO artists (
                artist_name
                )
            SELECT
                artist_name
            FROM artists_temp;
        """
    )


def upsert_streams(
    **kwargs
):
    connection_id = kwargs["redshift_conn_id"]
    redshift = PostgresHook(connection_id)
    date = kwargs["execution_date"].strftime("%Y-%m-%d")

    redshift.run(
        f"""
            
        """
    )



default_args = {
    "owner": "adeniyi",
    "start_date": datetime(2017, 1, 10),
    "end_date": datetime(2017, 1, 31),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": False,
    "catchup": True
}

dag = DAG(
    "capstone_project",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="@daily"
    # schedule_interval=None
    )

start_operator = DummyOperator(
    task_id="start_operator",
    dag=dag
)

create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

stage_streams_redshift = PythonOperator(
    task_id="stage_streams_redshift",
    dag=dag,
    python_callable=stage_streams,
    provide_context=True,
    op_kwargs={
        "redshift_conn_id": "redshift",
        "table_name": "streams_staging",
        "s3_bucket": "adeniyi-capstone-project",
        "s3_key": "kaggle_data"
    }
)

enrich_streams_data = PythonOperator(
    task_id="enrich_streams_data",
    dag=dag,
    python_callable=enrich_streams_data,
    provide_context=True,
    op_kwargs={
        "redshift_conn_id": "redshift",
        "table_name": "streams_staging"
    }
)

metadata_to_redshift = PythonOperator(
    task_id="metadata_to_redshift",
    dag=dag,
    python_callable=s3_to_redshift,
    provide_context=True,
    op_kwargs={
        "redshift_conn_id": "redshift",
        "table_name": "tracks_metadata",
        "s3_bucket": "adeniyi-capstone-project",
        "s3_key": "track_metadata/track_metadata"
    }
)

start_operator >> create_tables
create_tables >> stage_streams_redshift
stage_streams_redshift >> enrich_streams_data
enrich_streams_data >> metadata_to_redshift
