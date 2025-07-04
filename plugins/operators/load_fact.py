from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                aws_credintials_id="", # Define your operators params (with defaults) here
                redshift_conn_id="",
                sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credintials_id,# Map params here
        self.redshift_conn_id + redshift_conn_id, # Example:
        self.sql.query = sql_query,# self.conn_id = conn_id

    def execute(self, context):
        self.log.info(f"Loading Data {self.table_name}")

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift_hook.run(str(self.sql_query))
