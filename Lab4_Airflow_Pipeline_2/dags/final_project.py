from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from plugins.operators.stage_redshift import StageToRedshiftOperator
from plugins.operators.load_fact import LoadFactOperator
from plugins.operators.load_dimension import LoadDimensionOperator
from plugins.operators.data_quality import DataQualityOperator
from plugins.operators.create_tables import CreateTableOperator
from helpers import final_project_sql_statements
from helpers.final_project_sql_statements import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator
import configparser
from airflow.models import Variable

config = configparser.ConfigParser()

config.read('/home/workspace/airflow/dags/cd0031-automate-data-pipelines/project/starter/data.cfg')

log_jsonpath = config.get('S3', 'LOG_JSONPATH')


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depdends_on_past': False,
    'retries': 3,
    'retry_delay':timedelta(minutes=5),
    'catchup': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
    max_active_runs = 1
)

def final_project():

    start_operator = CreateTableOperator(
        task_id='Begin_execution',
        redshift_conn_id = 'redshift',
        sql_file_path = '/home/workspace/airflow/dags/cd0031-automate-data-pipelines/project/starter/create_tables.sql'
        )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id ='redshift',
        aws_credential_id = 'aws_credentials',
        table = 'staging_events',
        s3_bucket = 'udacity-dend',
        s3_key = 'log_data',
        json_option=log_jsonpath,
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id = 'redshift',
        aws_credential_id = 'aws_credentials',
        table = 'staging_songs',
        s3_bucket = 'udacity-dend',
        s3_key = 'song_data',
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id = 'redshift',
        table = 'songplay_table',
        sql_query = SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id = 'redshift',
        table = 'user_table',
        sql_query = SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id = 'redshift',
        table = 'song_table',
        sql_query = SqlQueries.song_table_insert 
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id = 'redshift',
        table = 'artist_table',
        sql_query = SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id = 'redshift',
        table = 'time_table',
        sql_query = SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id = 'redshift',
        tables = ['songplay_table', 'user_table', 'song_table', 'artist_table', 'time_table' ]
    )

    end_operator = DummyOperator(task_id='End_execution')


# DAG implementation
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, \
    load_song_dimension_table, \
    load_artist_dimension_table, \
    load_time_dimension_table ]

    [load_user_dimension_table, \
    load_song_dimension_table, \
    load_artist_dimension_table, \
    load_time_dimension_table] >> run_quality_checks

    run_quality_checks >> end_operator

final_project_dag = final_project()
