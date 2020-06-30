import logging
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s : %(message)s"
    )


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"

    @apply_defaults
    def __init__(
        self,
        table_name,
        s3_bucket,
        s3_key,
        redshift_conn_id="redshift",
        aws_conn_id="aws_credentials",
        region="us-west-2",
        json_copy="auto",
        *args,
        **kwargs
    ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.query = ("""
            COPY {table_name}
            FROM '{s3_bucket}'
            ACCESS_KEY_ID '{key}'
            SECRET_ACCESS_KEY '{secret}'
            REGION AS '{region}'
            CSV;
        """)
        self.aws_hook = AwsHook(aws_conn_id)
        self.aws_credentials = self.aws_hook.get_credentials()
        self.redshift = PostgresHook(redshift_conn_id)
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_copy = json_copy

    def execute(
        self,
        context
    ):

        logging.info(
            "Running COPY command for {} table".format(self.table_name)
        )

        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        self.redshift.run(
            self.query.format(
                table_name=self.table_name,
                s3_bucket=s3_path,
                key=self.aws_credentials.access_key,
                secret=self.aws_credentials.secret_key,
                region=self.region,
                json_copy=self.json_copy
            )
        )
