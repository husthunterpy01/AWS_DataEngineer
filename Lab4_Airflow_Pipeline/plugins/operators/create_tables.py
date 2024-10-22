from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
import os

class CreateTableOperator(BaseOperator):
    @apply_defaults
    def __init__(self, redshift_conn_id, sql_file_path, *args, **kwargs):
        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_file_path = sql_file_path

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        with open(self.sql_file_path, 'r') as file:
            sql_commands = file.read()

        for sql in sql_commands.split(';'):
            sql = sql.strip()
            if sql:  
                self.log.info(f"Executing SQL: {sql}")
                redshift.run(sql)
