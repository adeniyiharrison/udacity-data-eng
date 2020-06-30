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


def dataframe_to_s3(
    s3_client,
    dataframe,
    year
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
        Key="spotify_data/tracks_data_{datetime}_{year}.csv".format(
            datetime=datetime.now().strftime("%Y%m%d%H%M%S"),
            year=year
        )
    )


def process_tracks(
    s3_client,
    query_results,
    year
):
    track_results = []

    for track in query_results["tracks"]["items"]:
        track_dict = {
            "song_uri": None,
            "song_name": None,
            "artist_name": None,
            "album_type": None,
            "album_release_date": None,
            "album_name": None,
            "popularity": None
        }
        try:
            track_dict["album_type"] = track["album"]["album_type"]
            track_dict["album_release_date"] = (
                track["album"]["release_date"]
            )
            track_dict["album_name"] = track["album"]["name"]
            track_dict["song_name"] = track["name"]
            track_dict["song_uri"] = track["uri"]
            track_dict["popularity"] = track["popularity"]

            if type(track["artists"]) == list:
                track_dict["artist_name"] = track["artists"][0]["name"]
            elif type(track["artists"]) == dict:
                track_dict["artist_name"] = track["artists"]["name"]

        except Exception as e:
            logging.info(e)

        track_results.append(track_dict)

    track_results = DataFrame(track_results)
    dataframe_to_s3(s3_client, track_results, year)


def query_tracks(
    **kwargs
):

    year = kwargs["execution_date"].strftime("%Y")

    spc = return_spotipy_client()
    s3 = return_s3_client()

    for i in range(0, 2000, 50):
        try:
            query_results = spc.search(
                q=f"year:{year}",
                type="track",
                limit=50,
                offset=i
            )
            logging.info(f"query results returned for index #{i}")
            process_tracks(
                s3_client=s3,
                query_results=query_results,
                year=year
            )
        except Exception as e:
            logging.info(e)
            pass


def s3_to_redshift(
    **kwargs
):
    connection_id = kwargs["redshift_conn_id"]
    redshift = PostgresHook(connection_id)
    key = Variable.get("aws_key")
    secret = Variable.get("aws_secret")

    year = kwargs["execution_date"].strftime("%Y")
    table_name = kwargs["table_name"]
    s3_bucket = kwargs["s3_bucket"]
    s3_key = kwargs["s3_key"]
    s3_path = "s3://{}/{}".format(s3_bucket, s3_key)

    redshift.run(
        f"""
        CREATE TEMP TABLE {table_name}_{year}_temp (LIKE {table_name});

        COPY {table_name}_{year}_temp
        FROM '{s3_path}'
        ACCESS_KEY_ID '{key}'
        SECRET_ACCESS_KEY '{secret}'
        REGION AS 'us-west-1'
        CSV;

        DELETE FROM {table_name}
        USING {table_name}_{year}_temp
        WHERE {table_name}.song_uri = {table_name}_{year}_temp.song_uri;

        INSERT INTO {table_name}
        SELECT *
        FROM {table_name}_{year}_temp;
        """
    )


default_args = {
    "owner": "adeniyi",
    "start_date": datetime(1960, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": False,
    "catchup": False
}

dag = DAG(
    "capstone_project",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="@yearly"
    )

start_operator = DummyOperator(
    task_id="start_operator",
    dag=dag
)

tracks_to_s3 = PythonOperator(
    task_id="tracks_to_s3",
    dag=dag,
    python_callable=query_tracks,
    provide_context=True
)

create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

stage_redshift = PythonOperator(
    task_id="stage_redshift",
    dag=dag,
    python_callable=s3_to_redshift,
    provide_context=True,
    op_kwargs={
        "redshift_conn_id": "redshift",
        "table_name": "tracks_staging",
        "s3_bucket": "adeniyi-capstone-project",
        "s3_key": "spotify_data"
    }
)

start_operator >> tracks_to_s3
tracks_to_s3 >> create_tables
create_tables >> stage_redshift
