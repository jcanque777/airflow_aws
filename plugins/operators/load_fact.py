from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_into_stmt="""
        INSERT INTO {table}
        {select_query}
    """

    truncate_stmt = """
        TRUNCATE TABLE {table}
    """
    
    @apply_defaults
    def __init__(self,
                aws_credentials_id="",
                redshift_conn_id="",
                table="",
                truncate_table="",
                select_query="",
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id=aws_credentials_id,
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate_table = truncate_table
        self.select_query = select_query

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        if self.truncate_table:
            redshift.run(LoadFactOperator.truncate_stmt.format(
                table=self.table
            ))
        
        redshift_hook.run(LoadFactOperator.insert_into_stmt.format(
            table=self.table,
            select_query=self.select_query
        ))
