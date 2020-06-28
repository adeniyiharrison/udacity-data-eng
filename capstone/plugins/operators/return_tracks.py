import logging
import spotipy
from spotipy import oauth2
from pandas import DataFrame
from airflow.models import Variable
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class ReturnTracksOperator(
    BaseOperator
):

    ui_color = "#89DA59"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s : %(message)s"
        )

    @apply_defaults
    def __init__(
        self,
        products_to_return=100,
        year="2020",
        *args,
        **kwargs
    ):

        super(ReturnTracksOperator, self).__init__(*args, **kwargs)
        self.spotipy_client = self.return_spotipy_client()
        self.products_to_return = products_to_return
        self.year = year

    def return_spotipy_client(
        self
    ):
        ccm = oauth2.SpotifyClientCredentials(
            client_id=Variable.get("spotify_client_id"),
            client_secret=Variable.get("spotify_client_secret")
        )

        spc = spotipy.Spotify(
            client_credentials_manager=ccm
        )

        return spc

    def process_tracks(
        self,
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
                print(e)

            track_results.append(track_dict)

        track_results = DataFrame(track_results)

    def execute(
        self
    ):

        for i in range(0, self.products_to_return, 50):
            query_results = self.spotipy_client.search(
                q=f"year:{self.year}",
                type="track",
                limit=50,
                offset=i
            )

            logging.info(f"query results returned for index #{i}")

            track_results = self.process_tracks(query_results)

            logging.info(track_results)
