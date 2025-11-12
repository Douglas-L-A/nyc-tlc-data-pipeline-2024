SELECT
    f.trip_id,
    v.VendorID,
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
FROM silver.fact_table AS f
JOIN silver.vendor_dim AS v ON f.vendor_id = v.vendor_id
JOIN silver.datetime_dim AS d ON f.datetime_id = d.datetime_id
JOIN silver.passenger_count_dim AS p ON f.passenger_count_id = p.passenger_count_id
JOIN silver.trip_distance_dim AS t ON f.trip_distance_id = t.trip_distance_id
JOIN silver.rate_code_dim AS r ON f.rate_code_id = r.rate_code_id
JOIN silver.payment_type_dim AS pay ON f.payment_type_id = pay.payment_type_id;
