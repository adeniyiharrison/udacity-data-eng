from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Loads fact_table in Redshift from data in staging table

    """

    ui_color = "#F98866"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql,
                 *args,
                 **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift = PostgresHook(redshift_conn_id)
        self.table = table
        self.sql = sql

    def execute(
        self,
        context
    ):

        self.redshift.run(
            f"""
            DELETE FROM {self.table}
            """
        )

        self.log.info(
            "Loading data to fact table!"
        )
        self.redshift.run(
            f"""
            INSERT INTO {self.table}
            {self.sql}
            """
        )
