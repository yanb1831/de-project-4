from psycopg2.extras import RealDictCursor


class RestaurantsStgData:
    def list_restaurants(self, connection) -> str:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                    SELECT json_array_elements((value::JSON ->> 'objects')::JSON) ->> '_id' AS _id,
                           json_array_elements((value::JSON ->> 'objects')::JSON) ->> 'name' AS name
                    FROM stg.restaurants
                    ORDER BY 1
                """
            )
            data = cursor.fetchall()
            return data


class RestaurantsDdsSaver:
    def insert_dds_data(self, connection, restaurant) -> None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                    INSERT INTO dds.restaurants (
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
                    "_id": restaurant["_id"],
                    "name": restaurant["name"]
                }
            )


class RunRestaurantsDdsSaver:
    def __init__(self, pg_connect):
        self.pg = pg_connect
        self.stg = RestaurantsStgData()
        self.dds_saver = RestaurantsDdsSaver()

    def load_restaurants(self):
        with self.pg.connection() as connection:
            data = self.stg.list_restaurants(connection)

            for restaurant in data:
                self.dds_saver.insert_dds_data(connection, restaurant)
