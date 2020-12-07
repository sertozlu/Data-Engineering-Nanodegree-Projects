from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_credential_id="",
                table_name = "",
                s3_bucket="",
                s3_key = "",
                file_format = "",
                json_path = "",
                *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.json_path = json_path
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Staging file for {self.table_name} table")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        formatted_sql = self.copy_sql.format(
            self.table_name,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.file_format,
            self.json_path
        )
        redshift.run(formatted_sql)
        self.log.info(f"Staged {self.table_name} successfully")

