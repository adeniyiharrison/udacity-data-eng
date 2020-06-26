from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 sql,
                 compare_result,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift = PostgresHook(redshift_conn_id)
        self.sql = sql
        self.compare_result = compare_result

    def execute(self, context):

        records = self.redshift.get_records(self.sql)
        if records[0][0] != self.compare_result:
            raise ValueError("Data quality check failed")
        else:
            self.log.info("Data quality check passed")
