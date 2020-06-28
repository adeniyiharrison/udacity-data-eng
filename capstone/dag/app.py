import boto3
import logging
from io import StringIO
import spotipy
from spotipy import oauth2
from pandas import DataFrame
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
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
    dataframe
):

    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer)

    s3_client.put_object(
        Body=csv_buffer.getvalue(),
        Bucket="adeniyi-capstone-project",
        Key="spotify_data/tracks_data_{datetime}.csv".format(
            datetime=datetime.now().strftime("%Y%m%d%H%M")
        )
    )


def process_tracks(
    s3_client,
    query_results
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
    dataframe_to_s3(s3_client, track_results)


def query_tracks(
    products_to_return=100,
    year="2020"
):

    spc = return_spotipy_client()
    s3 = return_s3_client()

    for i in range(0, products_to_return, 50):
        query_results = spc.search(
            q=f"year:{year}",
            type="track",
            limit=50,
            offset=i
        )

        logging.info(f"query results returned for index #{i}")
        process_tracks(
            s3_client=s3,
            query_results=query_results
        )


default_args = {
    "owner": "adeniyi",
    "start_date": datetime(2020, 6, 28),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": False,
    "catchup": False
}

dag = DAG(
    "capstone_project",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="@daily"
    )

start_operator = DummyOperator(
    task_id="Begin_execution",
    dag=dag
)

copy_trips_task = PythonOperator(
    task_id="tracks_to_s3",
    dag=dag,
    python_callable=query_tracks
)

start_operator >> copy_trips_task
