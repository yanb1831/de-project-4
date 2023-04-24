import pendulum
from utilites.file_reader import SqlFileReader
from utilites.pg_connect import ConnectionBuilder
from airflow.models.variable import Variable
from airflow.decorators import dag, task


@dag(
    schedule_interval="* * 1 * *",
    start_date=pendulum.datetime(2023, 4, 19, tz="UTC"),
    catchup=False
)
def project_init_tables():
    pg_connect = ConnectionBuilder.pg_conn("PG_CONNECT")

    path = Variable.get("INIT_TABLES_PATH")

    @task(task_id="init_tables_task")
    def tables_init():
        ddl = SqlFileReader(pg_connect)
        ddl.sql_init(path)

    tables_init = tables_init()

    (
        tables_init
    )


project_init_tables = project_init_tables()
