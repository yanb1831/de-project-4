import pendulum

from utilites.yandex_api import YandexApi
from utilites.file_reader import SqlFileReader
from utilites.pg_connect import ConnectionBuilder

from dds.dds_couriers_saver import RunCouriersDdsSaver
from dds.dds_restaurants_saver import RunRestaurantsDdsSaver
from dds.dds_deliveries_saver import RunDeliveriesDdsSaver
from stg.stg_data_saver import RunStgDataSaver
from cdm.cdm_saver import RunReportCdmDataSaver

from airflow.decorators import dag, task, task_group
from airflow.models.variable import Variable


@dag(
    schedule_interval="* 2 * * *",
    start_date=pendulum.datetime(2023, 4, 19, tz="UTC"),
    catchup=False
)
def project_dag():
    pg_connect = ConnectionBuilder.pg_conn("PG_CONNECT")
    api = YandexApi(
        nickname=Variable.get("Nickname"),
        cohort=Variable.get("Cohort"),
        api_key=Variable.get("Key")
    )
    saver = RunStgDataSaver(pg_connect, api)
    path = Variable.get("CLEAR_TABLES_PATH")

    @task_group
    def get_data_tasks_group():
        @task
        def get_data_restaurants():
            saver.load_data("restaurants")

        @task
        def get_data_couriers():
            saver.load_data("couriers")

        @task
        def get_data_deliveries():
            saver.load_data("deliveries")

        get_data_restaurants = get_data_restaurants()
        get_data_couriers = get_data_couriers()
        get_data_deliveries = get_data_deliveries()

    @task
    def clear_stg_data_tables_last_1_days():
        dml = SqlFileReader(pg_connect)
        dml.sql_init(path)

    @task_group
    def stg_to_dds_group():
        @task
        def stg_to_dds_couriers_data():
            insert_data = RunCouriersDdsSaver(pg_connect)
            insert_data.load_couriers()

        @task
        def stg_to_dds_restaurants_data():
            insert_data = RunRestaurantsDdsSaver(pg_connect)
            insert_data.load_restaurants()

        @task
        def stg_to_dds_deliveries_data():
            insert_data = RunDeliveriesDdsSaver(pg_connect)
            insert_data.load_deliveries()

        stg_to_dds_couriers_data = stg_to_dds_couriers_data()
        stg_to_dds_restaurants_data = stg_to_dds_restaurants_data()
        stg_to_dds_deliveries_data = stg_to_dds_deliveries_data()

    @task
    def dds_to_cdm_report_data():
        insert_data = RunReportCdmDataSaver(pg_connect)
        insert_data.load_reports()

    clear_stg_data_tables_last_1_days = clear_stg_data_tables_last_1_days()
    stg_to_dds_group = stg_to_dds_group()
    get_data_tasks_group = get_data_tasks_group()
    dds_to_cdm_report_data = dds_to_cdm_report_data()

    (
        get_data_tasks_group
        >> clear_stg_data_tables_last_1_days
        >> stg_to_dds_group
        >> dds_to_cdm_report_data
    )


project_dag = project_dag()
