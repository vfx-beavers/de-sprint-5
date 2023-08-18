from logging import Logger

from lib import PgConnect
from psycopg import Connection


class CourierLedgerRepository:
    def __init__(self, log: Logger):
        self.log = log

    def load_courier_ledger(self, conn: Connection) -> None:

        with conn.cursor() as cur:
            cur.execute(
                """
                    WITH step1_courier_calc AS
                        (SELECT c.courier_id, c.courier_name, o.order_id, o.sum, t.year, t.month, avg(d.rate)::float AS rate_avg,
	        CASE WHEN avg(d.rate) < 4 THEN (CASE WHEN 0.05 * o.sum <= 100 THEN 100 ELSE 0.05 * o.sum END)
	            WHEN avg(d.rate) >= 4 AND avg(d.rate) < 4.5 THEN (CASE WHEN 0.07 * o.sum <= 150 THEN 150 ELSE 0.07 * o.sum END)
	            WHEN avg(d.rate) >= 4.5 AND avg(d.rate) < 4.9 THEN (CASE WHEN 0.08 * o.sum <= 175 THEN 175 ELSE 0.08 * o.sum END)
	            WHEN avg(d.rate) >= 4.9 THEN (CASE WHEN 0.1 * o.sum <= 200 THEN 200 ELSE 0.1 * o.sum END)
	        END AS courier_order_sum
                    FROM dds.cs_deliveries d
                    INNER JOIN dds.cs_couriers c ON d.courier_id = c.id
                    INNER JOIN dds.cs_orders o ON d.order_id = o.id
                    INNER JOIN dds.cs_timestamps t ON t.ts = o.order_ts
                    GROUP BY c.courier_id, c.courier_name, o.order_id, o.sum, t.year, t.month),
                    
                    step2_agg_tips_calc AS
                        (SELECT courier_id, courier_name, "year", "month", SUM(courier_order_sum) AS courier_order_sum
    FROM step1_courier_calc
    GROUP BY courier_id, courier_name, "year", "month")
                                
                    INSERT INTO cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month, orders_count, order_total_sum, 
    	rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
	    
    	SELECT c.courier_id, c.courier_name, t.year, t.month, count(d.delivery_id) AS orders_count,
	        sum(o."sum") AS orders_total_sum, round(avg(d.rate)::numeric, 2) AS rate_avg,
	        sum(o."sum") * 0.25 AS order_processing_fee, t3.courier_order_sum AS courier_order_sum,
	        sum(d.tip_sum) AS courier_tips_sum, (t3.courier_order_sum + 0.95 * sum(d.tip_sum)) AS courier_reward_sum
	    FROM dds.cs_deliveries d
	    INNER JOIN dds.cs_couriers c ON d.courier_id = c.id
	    INNER JOIN dds.cs_orders o ON d.order_id = o.id
	    INNER JOIN dds.cs_timestamps t ON o.order_ts = t.ts
	    INNER JOIN step2_agg_tips_calc AS t3 ON 
	    	(c.courier_id = t3.courier_id AND t.year = t3.year AND t.month = t3.month)
	    GROUP BY c.courier_id, c.courier_name, t.year, t.month, courier_order_sum  
                            
                    ON CONFLICT (courier_id, settlement_year, settlement_month) 
                    do UPDATE
                    SET courier_name = excluded.courier_name,
                        orders_count = excluded.orders_count,
                        order_total_sum = excluded.order_total_sum,
                        rate_avg = excluded.rate_avg,
                        order_processing_fee = excluded.order_processing_fee,
                        courier_order_sum = excluded.courier_order_sum,
                        courier_tips_sum = excluded.courier_tips_sum,
                        courier_reward_sum = excluded.courier_reward_sum;
                """, {}
            )

class CourierLedgerLoader:

    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.pg_conn = pg_conn
        self.cdm     = CourierLedgerRepository(log)
        self.log     = log

    def load_datamart(self):

        with self.pg_conn.connection() as conn:

            self.cdm.load_courier_ledger(conn)

            self.log.info(f"<<< Load finished")
