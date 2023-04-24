DELETE FROM stg.restaurants
WHERE update_ts <= current_date AT TIME ZONE 'UTC' - interval '1 days';
DELETE FROM stg.couriers
WHERE update_ts <= current_date AT TIME ZONE 'UTC' - interval '1 days';

DELETE FROM stg.deliveries
WHERE update_ts <= current_date AT TIME ZONE 'UTC' - interval '1 days';