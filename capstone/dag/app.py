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
from sql_queries import SqlQueries

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
        SqlQueries.count_records_query.format(
            table_name=table_name
        )
    )
    total_records = total_records[0][0]

    if total_records == 0:
        redshift.run(
            SqlQueries.streams_copy_query.format(
                table_name=table_name,
                s3_path=s3_path,
                key=key,
                secret=secret
            )
        )


def tracks_qa(
    **kwargs
):
    s3 = return_s3_client()
    date_str = kwargs["execution_date"].strftime("%Y-%m-%d")
    key = f"track_metadata/track_metadata_{date_str}.csv"

    objects = s3.list_objects(
        Bucket="adeniyi-capstone-project",
        Prefix="track_metadata"
    )
    objects = [x["Key"] for x in objects["Contents"]]

    logging.info(objects)

    if key in objects:
        pass
    else:
        raise ValueError("Data quality check failed")


def enrich_streams_data(
    **kwargs
):
    """
        Hit Spotify API via Spotipy library
    """

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

    date = kwargs["execution_date"].strftime("%Y-%m-%d")
    connection_id = kwargs["redshift_conn_id"]
    redshift = PostgresHook(connection_id)
    spc = return_spotipy_client()
    s3 = return_s3_client()

    daily_streams = redshift.get_records(
        SqlQueries.enrich_streams_query.format(
            date=date
        )
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
        key=f"track_metadata/track_metadata_{date}.csv"
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
        SqlQueries.s3_to_redshift_query.format(
            table_name=table_name,
            s3_path=s3_path,
            key=key,
            secret=secret
        )
    )


def streams_qa(
    **kwargs
):
    connection_id = kwargs["redshift_conn_id"]
    redshift = PostgresHook(connection_id)
    date = kwargs["execution_date"].strftime("%Y-%m-%d")

    total_rows = redshift.get_records(
        SqlQueries.streams_qa_qyery.format(
            date=date
        )
    )

    if int(total_rows[0][0]) > 0:
        pass
    else:
        raise ValueError("Data quality check failed")


def upsert_tracks(
    **kwargs
):
    connection_id = kwargs["redshift_conn_id"]
    redshift = PostgresHook(connection_id)
    date = kwargs["execution_date"].strftime("%Y-%m-%d")

    redshift.run(
        SqlQueries.upsert_tracks_query.format(
            date=date
        )
    )


def upsert_streams(
    **kwargs
):
    connection_id = kwargs["redshift_conn_id"]
    redshift = PostgresHook(connection_id)
    date = kwargs["execution_date"].strftime("%Y-%m-%d")

    redshift.run(
        SqlQueries.upsert_streams_query.format(
            date=date
        )
    )


default_args = {
    "owner": "adeniyi",
    "start_date": datetime(2017, 2, 1),
    "end_date": datetime(2017, 2, 2),
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

redshift_streams_qa = PythonOperator(
    task_id="redshift_streams_qa",
    dag=dag,
    python_callable=streams_qa,
    provide_context=True,
    op_kwargs={
        "redshift_conn_id": "redshift",
        "table_name": "streams_staging"
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

s3_tracks_qa = PythonOperator(
    task_id="s3_tracks_qa",
    dag=dag,
    python_callable=tracks_qa,
    provide_context=True
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

upsert_tracks = PythonOperator(
    task_id="upsert_tracks",
    dag=dag,
    python_callable=upsert_tracks,
    provide_context=True,
    op_kwargs={
        "redshift_conn_id": "redshift"
    }
)

upsert_streams = PythonOperator(
    task_id="upsert_streams",
    dag=dag,
    python_callable=upsert_streams,
    provide_context=True,
    op_kwargs={
        "redshift_conn_id": "redshift"
    }
)

end_operator = DummyOperator(
    task_id="end_operator",
    dag=dag
)

start_operator >> create_tables
create_tables >> stage_streams_redshift
stage_streams_redshift >> redshift_streams_qa
redshift_streams_qa >> enrich_streams_data
enrich_streams_data >> s3_tracks_qa
s3_tracks_qa >> metadata_to_redshift
metadata_to_redshift >> upsert_tracks
upsert_tracks >> upsert_streams
upsert_streams >> end_operator
