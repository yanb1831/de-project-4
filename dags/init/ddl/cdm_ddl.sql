CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
	id int GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1),
	courier_id varchar(255) NOT NULL,
	courier_name varchar(255) NOT NULL,
	settlement_year int NOT NULL,
	settlement_month int NOT NULL CHECK (settlement_month > 0 AND settlement_month <= 12),
	orders_count int NOT NULL,
	orders_total_sum numeric(14,2) NOT NULL,
	rate_avg numeric(14,2) NOT NULL,
	order_processing_fee numeric(14,2) NOT NULL,
	courier_order_sum numeric(14,2) NOT NULL,
	courier_tips_sum numeric(14,2) NOT NULL,
	courier_reward_sum numeric(14,2) NOT NULL,
	CONSTRAINT cdm_courier_ledge_id_pk PRIMARY KEY (_id),
	CONSTRAINT cdm_courier_id_unique UNIQUE (courier_id)
);