{{ config(
    materialized='table',
    location_root='s3://uber-pipeline-2025/gold/'
) }}

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
FROM {{ source('silver', 'fact_table') }} AS f
JOIN {{ source('silver', 'vendor_dim') }} AS v ON f.vendor_id = v.vendor_id
JOIN {{ source('silver', 'datetime_dim') }} AS d ON f.datetime_id = d.datetime_id
JOIN {{ source('silver', 'passenger_count_dim') }} AS p ON f.passenger_count_id = p.passenger_count_id
JOIN {{ source('silver', 'trip_distance_dim') }} AS t ON f.trip_distance_id = t.trip_distance_id
JOIN {{ source('silver', 'rate_code_dim') }} AS r ON f.rate_code_id = r.rate_code_id
JOIN {{ source('silver', 'payment_type_dim') }} AS pay ON f.payment_type_id = pay.payment_type_id;
