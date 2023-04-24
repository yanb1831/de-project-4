from psycopg2.extras import RealDictCursor


class ReportCdmData:
    def list_report(self, connection) -> str:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                    WITH t AS (
                        SELECT DISTINCT
                               courier_id,
                               name AS courier_name,
                               date_part('year', order_ts) AS settlement_year,
                               date_part('month', order_ts) AS settlement_month,
                               count(order_id) over(PARTITION BY date_part('month', order_ts)) AS orders_count,
                               sum(sum) over(PARTITION BY date_part('month', order_ts)) AS orders_total_sum,
                               round(avg(rate) over(PARTITION BY courier_id),3) AS rate_avg,
                               sum(sum) over(PARTITION BY date_part('month', order_ts)) * 0.25 AS order_processing_fee,
                               sum(sum) over(PARTITION BY courier_id) AS order_sum,
                               sum(tip_sum) over(PARTITION BY courier_id) AS courier_tips_sum
                        FROM dds.deliveries dd
                        JOIN dds.couriers dc ON dd.courier_id = dc._id),
                    t2 AS (
                        SELECT courier_id,
                               courier_name,
                               settlement_year,
                               settlement_month,
                               orders_count,
                               orders_total_sum,
                               rate_avg,
                               order_processing_fee,
                               CASE
                                    WHEN rate_avg < 4 THEN order_sum * 0.95
                                    WHEN 4 <= rate_avg AND rate_avg < 4.5 THEN order_sum * 0.93
                                    WHEN 4.5 <= rate_avg AND rate_avg < 4.9 THEN order_sum * 0.92
                                    WHEN 4.9 <= rate_avg THEN order_sum * 0.90
                               END AS courier_order_sum,
                               courier_tips_sum
                        FROM t)
                    SELECT courier_id,
                           courier_name,
                           settlement_year,
                           settlement_month,
                           orders_count,
                           orders_total_sum,
                           rate_avg,
                           order_processing_fee,
                           courier_order_sum,
                           courier_tips_sum,
                           courier_order_sum + courier_tips_sum * 0.95 AS courier_reward_sum
                    FROM t2
                """
            )
            data = cursor.fetchall()
            return data


class ReportCdmDataSaver:
    def insert_cdm_data(self, connection, report) -> None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger (
                        courier_id,
                        courier_name,
                        settlement_year,
                        settlement_month,
                        orders_count,
                        orders_total_sum,
                        rate_avg,
                        order_processing_fee,
                        courier_order_sum,
                        courier_tips_sum,
                        courier_reward_sum
                    )
                    VALUES(
                        %(courier_id)s,
                        %(courier_name)s,
                        %(settlement_year)s,
                        %(settlement_month)s,
                        %(orders_count)s,
                        %(orders_total_sum)s,
                        %(rate_avg)s,
                        %(order_processing_fee)s,
                        %(courier_order_sum)s,
                        %(courier_tips_sum)s,
                        %(courier_reward_sum)s
                    )
                    ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
                    SET
                        courier_name = EXCLUDED.courier_name,
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        rate_avg = EXCLUDED.rate_avg,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        courier_order_sum = EXCLUDED.courier_order_sum,
                        courier_reward_sum = EXCLUDED.courier_reward_sum
                """,
                {
                    "courier_id": report["courier_id"],
                    "courier_name": report["courier_name"],
                    "settlement_year": report["settlement_year"],
                    "settlement_month": report["settlement_month"],
                    "orders_count": report["orders_count"],
                    "orders_total_sum": report["orders_total_sum"],
                    "rate_avg": report["rate_avg"],
                    "order_processing_fee": report["order_processing_fee"],
                    "courier_order_sum": report["courier_order_sum"],
                    "courier_tips_sum": report["courier_tips_sum"],
                    "courier_reward_sum": report["courier_reward_sum"]
                }
            )


class RunReportCdmDataSaver:
    def __init__(self, pg_connect):
        self.pg = pg_connect
        self.stg = ReportCdmData()
        self.dds_saver = ReportCdmDataSaver()

    def load_reports(self):
        with self.pg.connection() as connection:
            data = self.stg.list_report(connection)

            for report in data:
                self.dds_saver.insert_cdm_data(connection, report)
