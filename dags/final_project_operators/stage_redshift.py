from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
 
    ui_color = '#358140'
    template_fields = ("s3_path",) 

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id="",
                 table = "",
                 sql_init_command = "",
                 s3_path = "",
                 region= "",
                 file_format = "JSON",
                 copy_options = "",
                 *args, **kwargs):
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.sql_init_command = sql_init_command
        self.s3_path = s3_path
        self.copy_options = copy_options
        self.region = region
        self.file_format = file_format
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Drop and creates {self.table} stage table in Redshift")
        redshift.run(self.sql_init_command)
        rendered_s3_path = self.s3_path.format(**context)    
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_s3_path)
        self.log.info("Copying data from S3 to Redshift")
        formatted_sql = f"""
            COPY {self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'          
            {self.copy_options}
            STATUPDATE ON
            REGION '{self.region}';
            """
        redshift.run(formatted_sql)