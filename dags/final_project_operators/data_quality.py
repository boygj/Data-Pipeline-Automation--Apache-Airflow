from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Airflow operator for performing data quality checks on a Redshift table.

    Args:
        redshift_conn_id (str): Airflow connection ID for the Redshift cluster.
        tables (list): List of table names to check.
        dq_checks (list): List of dictionaries defining data quality checks. Each
                         dictionary should have:
                            - 'check_sql': SQL query to execute the check.
                            - 'expected_result': Expected result of the query.
    """
    
    ui_color = '#89DA59'  # Custom color for the operator in Airflow UI

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        # Run Custom Data Quality Checks
        for check in self.dq_checks:
            sql = check.get('check_sql')
            expected_result = check.get('expected_result')

            self.log.info(f"Running data quality check: {sql}")
            records = redshift_hook.get_records(sql)

            if not records or len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. No results returned for query: {sql}")
            
            result = records[0][0]
            if expected_result != result:
                raise ValueError(f"""
                    Data quality check failed for query: {sql}. 
                    Expected: {expected_result}, Actual: {result}
                """)
            else:
                self.log.info(f"Data quality check passed for query: {sql}")
            
        # Check for Table Row Counts
        for table in self.tables:
            self.log.info(f"Checking row count for table: {table}")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            num_records = records[0][0]
            
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            else:
                self.log.info(f"Data quality check passed for table: {table} with {num_records} rows")
