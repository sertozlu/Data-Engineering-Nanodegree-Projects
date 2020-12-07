from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET

s3_bucket = "udacity-dend"
song_s3_key = "song_data"
log_s3_key = "log_data"
json_path = "log_json_path.json"

default_args = {
    'owner': 'sparkify',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag_name = 'sparkify_etl_dag'
dag = DAG(dag_name,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_in_redshift = CreateTablesOperator(
    task_id='Create_tables',
    redshift_conn_id="redshift",
    dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id="redshift",
    aws_credential_id="aws_credentials",
    table_name="staging_events",
    s3_bucket=s3_bucket,
    s3_key=log_s3_key,
    file_format="JSON",
    json_path = json_path,
    provideContext=True,
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id="redshift",
    aws_credential_id="aws_credentials",
    table_name="staging_songs",
    s3_bucket=s3_bucket,
    s3_key=song_s3_key,
    file_format="JSON",
    json_path = json_path,
    provideContext=True,
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    aws_credential_id="aws_credentials",
    table="public.songplays",
    sql_query = SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    aws_credential_id="aws_credentials",
    table="public.users",
    sql_query = SqlQueries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    aws_credential_id="aws_credentials",
    table="public.songs",
    sql_query = SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    aws_credential_id="aws_credentials",
    table="public.artists",
    sql_query = SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    aws_credential_id="aws_credentials",
    table="public.time",
    sql_query = SqlQueries.time_table_insert,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    aws_credential_id="aws_credentials",
    tables=["songplays", "users", "songs", "artists", "time"],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task dependencies
start_operator >> create_tables_in_redshift
create_tables_in_redshift >> [stage_events_to_redshift, 
                                stage_songs_to_redshift] 
                        >> load_songplays_table

load_songplays_table >> [load_song_dimension_table, 
                        load_user_dimension_table,
                        load_artist_dimension_table, 
                        load_time_dimension_table] 
                    >> run_quality_checks

run_quality_checks >> end_operator
