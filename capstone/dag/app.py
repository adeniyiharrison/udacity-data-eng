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

    year = kwargs["year"]

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
            continue


def s3_to_redshift(
    **kwargs
):
    connection_id = kwargs["redshift_conn_id"]
    table_name = kwargs["table_name"]
    s3_bucket = kwargs["s3_bucket"]
    s3_key = kwargs["s3_key"]

    redshift = PostgresHook(connection_id)

    s3_path = "s3://{}/{}".format(s3_bucket, s3_key)

    query = (
        """
            COPY {table_name}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{key}'
            SECRET_ACCESS_KEY '{secret}'
            REGION AS 'us-west-1'
        """.format(
            table_name=table_name,
            s3_path=s3_path,
            key=Variable.get("aws_key"),
            secret=Variable.get("aws_secret")
        )
    )

    redshift.run(query)


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

copy_trips_task_2010 = PythonOperator(
    task_id="tracks_to_s3_2010",
    dag=dag,
    python_callable=query_tracks,
    op_kwargs={"year": "2010"}
)

copy_trips_task_2011 = PythonOperator(
    task_id="tracks_to_s3_2011",
    dag=dag,
    python_callable=query_tracks,
    op_kwargs={"year": "2011"}
)

copy_trips_task_2012 = PythonOperator(
    task_id="tracks_to_s3_2012",
    dag=dag,
    python_callable=query_tracks,
    op_kwargs={"year": "2012"}
)

copy_trips_task_2013 = PythonOperator(
    task_id="tracks_to_s3_2013",
    dag=dag,
    python_callable=query_tracks,
    op_kwargs={"year": "2013"}
)

copy_trips_task_2014 = PythonOperator(
    task_id="tracks_to_s3_2014",
    dag=dag,
    python_callable=query_tracks,
    op_kwargs={"year": "2014"}
)

copy_trips_task_2015 = PythonOperator(
    task_id="tracks_to_s3_2015",
    dag=dag,
    python_callable=query_tracks,
    op_kwargs={"year": "2015"}
)

copy_trips_task_2016 = PythonOperator(
    task_id="tracks_to_s3_2016",
    dag=dag,
    python_callable=query_tracks,
    op_kwargs={"year": "2016"}
)

copy_trips_task_2017 = PythonOperator(
    task_id="tracks_to_s3_2017",
    dag=dag,
    python_callable=query_tracks,
    op_kwargs={"year": "2017"}
)

copy_trips_task_2018 = PythonOperator(
    task_id="tracks_to_s3_2018",
    dag=dag,
    python_callable=query_tracks,
    op_kwargs={"year": "2018"}
)

copy_trips_task_2019 = PythonOperator(
    task_id="tracks_to_s3_2019",
    dag=dag,
    python_callable=query_tracks,
    op_kwargs={"year": "2019"}
)

copy_trips_task_2020 = PythonOperator(
    task_id="tracks_to_s3_2020",
    dag=dag,
    python_callable=query_tracks,
    op_kwargs={"year": "2020"}
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
    op_kwargs={
        "redshift_conn_id": "redshift",
        "table_name": "tracks_staging",
        "s3_bucket": "adeniyi-capstone-project",
        "s3_key": "spotify_data"
    }
)

start_operator >> [
    copy_trips_task_2010,
    copy_trips_task_2011,
    copy_trips_task_2012,
    copy_trips_task_2013,
    copy_trips_task_2014,
    copy_trips_task_2015,
    copy_trips_task_2016,
    copy_trips_task_2017,
    copy_trips_task_2018,
    copy_trips_task_2019,
    copy_trips_task_2020
    ]
[
    copy_trips_task_2010,
    copy_trips_task_2011,
    copy_trips_task_2012,
    copy_trips_task_2013,
    copy_trips_task_2014,
    copy_trips_task_2015,
    copy_trips_task_2016,
    copy_trips_task_2017,
    copy_trips_task_2018,
    copy_trips_task_2019,
    copy_trips_task_2020
    ] >> create_tables
create_tables >> stage_redshift
