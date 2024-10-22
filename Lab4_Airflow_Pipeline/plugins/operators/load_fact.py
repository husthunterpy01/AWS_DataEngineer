from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id = '',
                table = '',
                sql_query = '',
                mode = 'insert-data',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.mode = mode

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if (self.mode == "truncate-data"):
            redshift_hook.run(f"TRUNCATE TABLE {self.table}")
            self.log.info(f"Truncating the fact table {self.table}")
        
        sql_statement = f"INSERT INTO {self.table} {self.sql_query}"
        redshift_hook.run(sql_statement)
        self.log.info(f"Inserting the fact table {self.table}")