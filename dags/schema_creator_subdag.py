from airflow.decorators import task_group
from airflow.providers.postgres.operators.postgres import PostgresOperator
from helpers import SqlQueries  # Make sure SqlQueries is available


@task_group(group_id='sparkify_schema_creator')
def sparkify_schema_creator_dag(
    drop_db_tables_if_exists=False,
    redshift_conn_id="redshift"
):
    
    # Drop DB Schema (if enabled)
    if drop_db_tables_if_exists:
        drop_db_schema = PostgresOperator(
            task_id="drop_db_schema",
            postgres_conn_id=redshift_conn_id,
            sql=SqlQueries.db_schema_drop
        )

    # Create Dimension Tables
    create_user_table = PostgresOperator(
        task_id="create_user_table",
        postgres_conn_id=redshift_conn_id,
        sql=SqlQueries.user_table_create
    )

    create_artist_table = PostgresOperator(
        task_id="create_artist_table",
        postgres_conn_id=redshift_conn_id,
        sql=SqlQueries.artist_table_create
    )

    create_time_table = PostgresOperator(
        task_id="create_time_table",
        postgres_conn_id=redshift_conn_id,
        sql=SqlQueries.time_table_create
    )

    create_song_table = PostgresOperator(
        task_id="create_song_table",
        postgres_conn_id=redshift_conn_id,
        sql=SqlQueries.song_table_create
    )

    # Create Fact Table
    create_songplay_table = PostgresOperator(
        task_id="create_songplay_table",
        postgres_conn_id=redshift_conn_id,
        sql=SqlQueries.songplay_table_create
    )

    # Define Task Dependencies within TaskGroup
    if drop_db_tables_if_exists:
        drop_db_schema >> [create_user_table, create_artist_table, create_time_table]
    else:
        [create_user_table, create_artist_table, create_time_table]

    [create_user_table, create_artist_table, create_time_table] >> create_song_table
    create_song_table >> create_songplay_table  

