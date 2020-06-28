import configparser
from pandas import DataFrame
from datetime import datetime, timedelta

import spotipy
from spotipy import oauth2

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator


def return_spotipy_client():
    config = configparser.ConfigParser()
    config.read("config.cfg")
    client_id = config.get("SPOTIFY", "CLIENT_ID")
    client_secret = config.get("SPOTIFY", "CLIENT_SECRET")
    ccm = oauth2.SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret
    )

    spc = spotipy.Spotify(client_credentials_manager=ccm)

    return spc


def return_tracks(
    spotipy_client
):

    for i in range(0, 100, 50):
        track_results = spotipy_client.search(
            q="year:2020",
            type="track",
            limit=50,
            offset=i
        )

        track_results = []

        for track in track_results["tracks"]["items"]:
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
                print(e)

            track_results.append(track_dict)

        track_results = DataFrame(track_results)


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
