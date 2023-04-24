from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta


class DeliveriesStgData:
    def list_deliveries(self, connection) -> str:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                    SELECT json_array_elements((value::JSON ->> 'objects')::JSON) ->> 'order_id' AS order_id,
                           json_array_elements((value::JSON ->> 'objects')::JSON) ->> 'order_ts' AS order_ts,
                           json_array_elements((value::JSON ->> 'objects')::JSON) ->> 'delivery_id' AS delivery_id,
                           json_array_elements((value::JSON ->> 'objects')::JSON) ->> 'courier_id' AS courier_id,
                           json_array_elements((value::JSON ->> 'objects')::JSON) ->> 'address' AS address,
                           json_array_elements((value::JSON ->> 'objects')::JSON) ->> 'delivery_ts' AS delivery_ts,
                           json_array_elements((value::JSON ->> 'objects')::JSON) ->> 'rate' AS rate,
                           json_array_elements((value::JSON ->> 'objects')::JSON) ->> 'sum' AS sum,
                           json_array_elements((value::JSON ->> 'objects')::JSON) ->> 'tip_sum' AS tip_sum
                    FROM stg.deliveries
                    ORDER BY 1;
                """
            )
            data = cursor.fetchall()
            return data


class DeliveriesDdsSaver:
    def insert_dds_data(self, connection, deliveries) -> None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                    INSERT INTO dds.deliveries (
                        order_id,
                        order_ts,
                        delivery_id,
                        courier_id,
                        address,
                        delivery_ts,
                        rate,
                        sum,
                        tip_sum
                    )
                    VALUES(
                        %(order_id)s,
                        %(order_ts)s,
                        %(delivery_id)s,
                        %(courier_id)s,
                        %(address)s,
                        %(delivery_ts)s,
                        %(rate)s,
                        %(sum)s,
                        %(tip_sum)s
                    )
                    ON CONFLICT (order_id) DO UPDATE
                    SET
                        order_ts = EXCLUDED.order_ts,
                        delivery_id = EXCLUDED.delivery_id,
                        courier_id = EXCLUDED.courier_id,
                        address = EXCLUDED.address,
                        delivery_ts = EXCLUDED.delivery_ts,
                        rate = EXCLUDED.rate,
                        sum = EXCLUDED.sum,
                        tip_sum = EXCLUDED.tip_sum
                """,
                {
                    "order_id": deliveries["order_id"],
                    "order_ts": deliveries["order_ts"],
                    "delivery_id": deliveries["delivery_id"],
                    "courier_id": deliveries["courier_id"],
                    "address": deliveries["address"],
                    "delivery_ts": deliveries["delivery_ts"],
                    "rate": deliveries["rate"],
                    "sum": deliveries["sum"],
                    "tip_sum": deliveries["tip_sum"],
                }
            )


class RunDeliveriesDdsSaver:
    WF_KEY = "stg_to_dds_deliveries_timestamp"
    TIME = datetime.now() - timedelta(days=7)

    def __init__(self, pg_connect) -> None:
        self.pg = pg_connect
        self.stg = DeliveriesStgData()
        self.dds_saver = DeliveriesDdsSaver()

    def load_deliveries(self) -> None:
        with self.pg.connection() as connection:

            data = self.stg.list_deliveries(connection)

            for deliveries in data:
                self.dds_saver.insert_dds_data(connection, deliveries)
