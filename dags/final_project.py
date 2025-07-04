from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements
from airflow import Variable 
from airflow.utils.helpers import chain
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


default_args = {
    'owner': 'pcarona',
    'start_date': pendulum.now(),
    'retries': 3, 
    'retry_delay': timedate(minutes=10),
    'catchup': False,
    'depends_on_past': False,
}

s3_bucket = Variable.get('s3_bucket')

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@daily'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        redshift_conn_id= 'redshift',
        aws_credintials_id='aws_credentials',
        table= 'staging_events',
        s3_bucket= 'udacity-dend',
        s3_key= 'log_data',
        s3_path=f's3://{s3_bucket}/logdata',
        json_option=f's3://{s3_bucket}/log_json_path.json',
        task_id='Stage_events',
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credintials_id='aws_credintials',
        table='staging_songs',
        s3_bucket= S3_bucket,
        s3_key="song-data/A/A/A",
        s3_path=f"s3://{s3_bucket}/song-data/A/A/A",
        json_option='auto',
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        sql=sq.SqlQueries.songplay_table_insert,
        table="songplays",
        truncate=False,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table="users",
        sql=sq.SqlQueries.user_table_insert,
        truncate=False,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table="songs",
        sql=sq.SqlQueries.user_table_insert,
        truncate=False,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table="artists",
        sql=sq.SqlQueries.user_table_insert,
        truncate=False,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table="time",
        sql=sq.SqlQueries.user_table_insert,
        truncate=False,
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
    )

    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> [load_artist_dimension_table, load_song_dimension_table, load_time_dimension_table, load_user_dimension_table] >> run_quality_checks

final_project_dag = final_project()