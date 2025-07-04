from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                aws_credentials_id="", # Define your operators params (with defaults) here
                redshift_conn_id="", # Example:
                sql_query="", # conn_id = your-connection-name
                table="",
                truncate="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id # Map params here
        self.redshift_conn_id = redshift_conn_id# Example:
        self.sql_query = sql_query # self.conn_id = conn_id
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            redshift.run(f"TRUNCATE TABLE {self.table}")
        formatted_sql = self.sql_query.format(self.table)
        redshift.run(formatted_sql)
        self.log.info(f"success: {self.task.id} ")