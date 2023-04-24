CREATE TABLE IF NOT EXISTS stg.restaurants (
    id serial,
    update_ts timestamp WITHOUT TIME ZONE DEFAULT (now() AT TIME ZONE 'utc'),
    value text,
    CONSTRAINT stg_restaurants_id_pk PRIMARY KEY(id)
);
CREATE TABLE IF NOT EXISTS stg.couriers (
    id serial,
    update_ts timestamp WITHOUT TIME ZONE DEFAULT (now() AT TIME ZONE 'utc'),
    value text,
    CONSTRAINT stg_couriers_id_pk PRIMARY KEY(id)
);
CREATE TABLE IF NOT EXISTS stg.deliveries (
    id serial,
    update_ts timestamp WITHOUT TIME ZONE DEFAULT (now() AT TIME ZONE 'utc'),
    value text,
    CONSTRAINT stg_deliveries_id_pk PRIMARY KEY(id)
);