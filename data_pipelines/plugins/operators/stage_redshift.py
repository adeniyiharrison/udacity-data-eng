from airflow.models import Variable
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook

from . helpers.sql_queries import SqlQueries


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        aws_conn_id="aws_credentials",
        *args,
        **kwargs
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_hook = AwsHook(aws_conn_id)
        self.aws_credentials = self.aws_hook.get_credentials()
        self.redshift_hook = PostgresHook(redshift_conn_id)

    def execute(
        self
    ):

        self.redshift_hook.run(
            SqlQueries.create_table_from_s3.format(
                table_name="staging_events",
                s3_location=Variable.get("log_s3_bucket"),
                key=self.aws_credentials.access_key,
                secret=self.aws_credentials.secret_key
            )
        )

        self.redshift_hook.run(
            SqlQueries.create_table_from_s3.format(
                table_name="staging_songs",
                s3_location=Variable.get("songs_s3_bucket"),
                key=self.aws_credentials.access_key,
                secret=self.aws_credentials.secret_key
            )
        )
