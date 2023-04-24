CREATE TABLE IF NOT EXISTS dds.restaurants (
    _id varchar(255) NOT NULL,
    name varchar(255) NOT NULL,
    CONSTRAINT dds_restaurants_id_pk PRIMARY KEY(_id)
);
CREATE TABLE IF NOT EXISTS dds.couriers (
    _id varchar(255) NOT NULL,
    name varchar(255) NOT NULL,
    CONSTRAINT dds_couriers_id_pk PRIMARY KEY(_id)
);
CREATE TABLE IF NOT EXISTS dds.deliveries (
    id serial,
    order_id varchar(255) NOT NULL,
    order_ts timestamp NOT NULL,
    delivery_id varchar(255) NOT NULL,
    courier_id varchar(255) NOT NULL,
    address varchar(255) NOT NULL,
    delivery_ts timestamp NOT NULL,
    rate int NOT NULL,
    sum numeric(14,2) NOT NULL CHECK (sum >= 0),
    tip_sum numeric(14,2) NOT NULL CHECK (sum >= 0),
    CONSTRAINT dds_deliveries_id_pk PRIMARY KEY(id),
    CONSTRAINT dds_order_id_unique UNIQUE (order_id),
    CONSTRAINT dds_courier_id_fk FOREIGN KEY (courier_id) REFERENCES dds.couriers(_id)
);