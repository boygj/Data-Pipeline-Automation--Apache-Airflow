from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
  
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "",
                 table = "",
                 sql = "",                 
                 truncate_table = False,
                 *args, **kwargs):
    
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate_table = truncate_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
    
        if self.truncate_table:
            self.log.info(f'Truncating dimension table: {self.table}')
            redshift.run(f'TRUNCATE TABLE {self.table};')
        else:
            self.log.info(f'Appending data to dimension table: {self.table}')

        self.log.info(f'Inserting data into dimension table: {self.table}')
        formatted_sql = f"""
            INSERT INTO {self.table}
            {self.sql};
        """
        redshift.run(formatted_sql)
        self.log.info("DONE.")