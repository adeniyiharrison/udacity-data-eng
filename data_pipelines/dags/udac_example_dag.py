from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
    PostgresOperator
)
from helpers import SqlQueries


default_args = {
    "owner": "adeniyi",
    "start_date": datetime(2019, 1, 12),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": False,
    "catchup": True
}

dag = DAG("udac_example_dag",
          default_args=default_args,
          description="Load and transform data in Redshift with Airflow",
          schedule_interval="0 * * * *"
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="sql/create_tables.sql"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="stage_events",
    dag=dag,
    table_name="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    region="us-west-2",
    json_copy="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="stage_songs",
    dag=dag,
    table_name="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    region="us-west-2"
)

load_songplays_table = LoadFactOperator(
    task_id="load_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql=SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="load_user_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql=SqlQueries.user_table_insert,
    append_insert=True,
    primary_key="userid"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="load_song_dim_table",
    dag=dag,    
    redshift_conn_id="redshift",
    table="songs",
    sql=SqlQueries.song_table_insert,
    append_insert=True,
    primary_key="songid"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="load_artist_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql=SqlQueries.artist_table_insert,
    append_insert=True,
    primary_key="artistid"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="load_time_dim_table",
    dag=dag,    
    redshift_conn_id="redshift",
    table="time",
    sql=SqlQueries.time_table_insert,
    append_insert=True,
    primary_key="start_time"
)

run_quality_checks = DataQualityOperator(
    task_id="run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    sql=(
        """
        SELECT COUNT(*)
        FROM songs
        WHERE songid IS NULL;
        """
    ),
    compare_result=0
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table
]
[
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table
] >> run_quality_checks
run_quality_checks >> end_operator
