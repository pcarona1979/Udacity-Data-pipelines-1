from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="", # Define your operators params (with defaults) here
                aws_credintials_id="", # Example:
                table="", # redshift_conn_id=your-connection-name
                s3_bucket="",
                s3_key="",
                region="",
                file_format="JSON",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credintials_id = aws_credintials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.file_format = file_format
        self.region = region
        self.execution_date = kwargs.get("execution_date")


    def execute(self, context):
        aws_hook = AwsHook(self.aws_credintials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        

        self.log.info("Clearing data from Redshift")
        redshift.run("DELETE FROM{}".format(self.table))

        self.log.info("Moving Data from S3 to Redshift")

        s3_path = "s3://{}".format(self.s3_bucket)
        if self.execution_date:
            year = self.execution_date.strftime("%Y")
            month = self.execution_date.strftime("%m")
            day = self.execution_date.strftime("%^d")
        s3_path = s3_path + '/' +self.s3_key
            

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.file_format,
            )

        redshift.run(formatted_sql)

        self.log.info(f"Succes: {self.table} moved to Redshift")