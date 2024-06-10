from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.data_quality import DataQualityOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from helpers.sql_queries import SqlQueries
from schema_creator_subdag import sparkify_schema_creator_dag

default_args = {
    'owner': 'Eugene',
    'start_date': pendulum.now(),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def final_project():
    start_operator = DummyOperator(task_id='Begin_execution')
    
    @task()
    def stage_events():
        return StageToRedshiftOperator(
            task_id='Stage_events',
            redshift_conn_id="redshift",
            aws_credentials_id="aws_credentials",
            table="staging_events",
            s3_bucket="alphatest1111",
            s3_key="log_data",
            json_path="s3://alphatest1111/log_json_path.json"
        ).execute(context=None)
    
    @task()
    def stage_songs():
        return StageToRedshiftOperator(
            task_id='Stage_songs',
            redshift_conn_id="redshift",
            aws_credentials_id="aws_credentials",
            table="staging_songs",
            s3_bucket="udacity-dend",
            s3_key="song_data/A/A/A",  
            json_path="s3://alphatest1111/log_json_path.json",  
        ).execute(context=None)

    @task()
    def load_songplays_table():
        return LoadFactOperator(
            task_id='Load_songplays_fact_table',
            redshift_conn_id="redshift",
            table="songplays",
            sql_statement=SqlQueries.songplay_table_insert
        ).execute(context=None)

    @task()
    def load_user_dimension_table():
        return LoadDimensionOperator(
            task_id='Load_user_dim_table',
            redshift_conn_id="redshift",
            table="users",
            sql_statement=SqlQueries.user_table_insert,
            truncate_table=True
        ).execute(context=None)

    @task()
    def load_song_dimension_table():
        return LoadDimensionOperator(
            task_id='Load_song_dim_table',
            redshift_conn_id="redshift",
            table="songs",
            sql_statement=SqlQueries.song_table_insert,
            truncate_table=True
        ).execute(context=None)

    @task()
    def load_artist_dimension_table():
        return LoadDimensionOperator(
            task_id='Load_artist_dim_table',
            redshift_conn_id="redshift",
            table="artists",
            sql_statement=SqlQueries.artist_table_insert,
            truncate_table=True
        ).execute(context=None)

    @task()
    def load_time_dimension_table():
        return LoadDimensionOperator(
            task_id='Load_time_dim_table',
            redshift_conn_id="redshift",
            table="time",
            sql_statement=SqlQueries.time_table_insert,
            truncate_table=True
        ).execute(context=None)

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tables=["songplays", "users", "songs", "artists", "time"],
        dq_checks=[
            {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
            {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0},
        ]
    )
    end_operator = DummyOperator(task_id='End_execution')

    stage_events_task = stage_events()
    stage_songs_task = stage_songs()

    load_songplays_table_task = load_songplays_table()

    load_user_dimension_table_task = load_user_dimension_table()
    load_song_dimension_table_task = load_song_dimension_table()
    load_artist_dimension_table_task = load_artist_dimension_table()
    load_time_dimension_table_task = load_time_dimension_table()

    # Task Dependencies
    start_operator >> [stage_events_task, stage_songs_task]
    [stage_events_task, stage_songs_task] >> load_songplays_table_task
    load_songplays_table_task >> [load_user_dimension_table_task, load_song_dimension_table_task, load_artist_dimension_table_task, load_time_dimension_table_task]
    [load_user_dimension_table_task, load_song_dimension_table_task, load_artist_dimension_table_task, load_time_dimension_table_task] >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()

