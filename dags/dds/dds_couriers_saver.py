from psycopg2.extras import RealDictCursor


class CouriersStgData:
    def list_couriers(self, connection) -> str:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                    SELECT json_array_elements((value::JSON ->> 'objects')::JSON) ->> '_id' AS _id,
                           json_array_elements((value::JSON ->> 'objects')::JSON) ->> 'name' AS name
                    FROM stg.couriers
                    ORDER BY 1
                """
            )
            data = cursor.fetchall()
            return data


class CouriersDdsSaver:
    def insert_dds_data(self, connection, courier) -> None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                    INSERT INTO dds.couriers (
                        _id,
                        name
                    )
                    VALUES(
                        %(_id)s,
                        %(name)s
                    )
                    ON CONFLICT (_id) DO UPDATE
                    SET
                        name = EXCLUDED.name
                """,
                {
                    "_id": courier["_id"],
                    "name": courier["name"]
                }
            )


class RunCouriersDdsSaver:
    def __init__(self, pg_connect):
        self.pg = pg_connect
        self.stg = CouriersStgData()
        self.dds_saver = CouriersDdsSaver()

    def load_couriers(self):
        with self.pg.connection() as connection:
            data = self.stg.list_couriers(connection)

            for courier in data:
                self.dds_saver.insert_dds_data(connection, courier)
