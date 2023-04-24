from psycopg2 import sql


class StgDataSaver:
    def insert_stg_data(self, connection, buffer, field) -> None:
        with connection.cursor() as cursor:
            query = sql.SQL(
                """
                    COPY stg.{field} (value) FROM STDIN
                """
            ).format(field=sql.Identifier(field))
            buffer.seek(0)
            cursor.copy_expert(query, buffer)


class RunStgDataSaver:
    def __init__(self, pg_connect, api) -> None:
        self.pg = pg_connect
        self.saver = StgDataSaver()
        self.api = api

    def load_data(self, field: str) -> None:
        with self.pg.connection() as connection:
            buffer = self.api.get_requests(field)
            self.saver.insert_stg_data(connection, buffer, field)
