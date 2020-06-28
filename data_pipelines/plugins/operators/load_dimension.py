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
                 append_insert=False,
                 primary_key="",
                 *args,
                 **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift = PostgresHook(redshift_conn_id)
        self.table = table
        self.sql = sql
        self.append_insert = append_insert
        self.primary_key = primary_key

    def execute(
        self,
        context
    ):

        self.log.info(
            "Loading data to dim table!"
        )

        if self.append_insert:
            self.redshift.run(
                f"""
                CREATE TEMP TABLE stage_{self.table} (LIKE {self.table});

                INSERT INTO stage_{self.table}
                {self.sql};

                DELETE FROM {self.table}
                USING stage_{self.table}
                WHERE {self.table}.{self.primary_key} = stage_{self.table}.{self.primary_key};

                INSERT INTO {self.table}
                SELECT *
                FROM stage_{self.table};
                """
            )
        else:
            self.redshift.run(
                f"""
                INSERT INTO {self.table}
                {self.sql}
                """
            )
