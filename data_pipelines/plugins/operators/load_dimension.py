from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql,
                 *args,
                 **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift = PostgresHook(redshift_conn_id)
        self.table = table
        self.sql = sql

    def execute(
        self,
        context
    ):

        self.log.info(
            "Loading data to dim table!"
        )

        self.redshift.run(
            f"""
            INSERT INTO {self.table}
            {self.sql}
            """
        )
