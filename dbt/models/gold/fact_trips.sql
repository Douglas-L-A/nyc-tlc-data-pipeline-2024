SELECT
f.trip_id,
v.vendor_type_name,
d.tpep_pickup_datetime,
d.tpep_dropoff_datetime,
p.passenger_count,
t.trip_distance,
r.rate_code_name,
pay.payment_type_name,
f.fare_amount,
f.extra,
f.mta_tax,
f.tip_amount,
f.tolls_amount,
f.improvement_surcharge,
f.total_amount
FROM
fact_table f
JOIN vendor_dim v ON f.vendor_id = v.vendor_id
JOIN datetime_dim d ON f.datetime_id = d.datetime_id
JOIN passenger_count_dim p ON f.passenger_count_id = p.passenger_count_id
JOIN trip_distance_dim t ON f.trip_distance_id = t.trip_distance_id
JOIN rate_code_dim r ON f.rate_code_id = r.rate_code_id
JOIN payment_type_dim pay ON f.payment_type_id = pay.payment_type_id