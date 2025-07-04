from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                aws_credentials_id="", # Define your operators params (with defaults) here
                redshift_conn_id="", # Example:
                tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id,# Map params here
        self.redshift_conn_id = redshift_conn_id,# Example:
        self.tables = tables# self.conn_id = conn_id

    def execute(self, context):
        self.log.info('Running quality check on data')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for test in self.test:
            check_sql = test['check_sql']
            expected_result = test['expected_result']

            self.log.info(f"Running SQL: {check_sql}")
            records = redshift_hook.get_records(check_sql)
            result = records[0][0]
            if result != expected_result:
                raise ValueError(
            f"Quality Check Failed")