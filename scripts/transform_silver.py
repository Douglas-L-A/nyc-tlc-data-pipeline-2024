import pandas as pd
import boto3
from io import BytesIO

s3_path = "s3://uber-pipeline-2025/bronze/yellow_tripdata_2024.parquet"
df = pd.read_parquet(s3_path, engine="pyarrow")

#Tratando os dados
df['trip_id'] = df.index

#Tabelas Dimensão

datetime_dim = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].copy()
datetime_dim['pick_minute'] = datetime_dim['tpep_pickup_datetime'].dt.minute
datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday

datetime_dim['drop_minute'] = datetime_dim['tpep_dropoff_datetime'].dt.minute
datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday

datetime_dim['datetime_id'] = datetime_dim.index

datetime_dim = datetime_dim[[
    'datetime_id',
    'tpep_pickup_datetime', 'tpep_dropoff_datetime',
    'pick_minute', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
    'drop_minute', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday',
    ]]


trip_distance_dim = df[['trip_distance']].drop_duplicates().reset_index(drop=True)
trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
trip_distance_dim = trip_distance_dim[['trip_distance_id', 'trip_distance']]


passenger_count_dim = df[['passenger_count']].fillna(1).drop_duplicates().reset_index(drop=True).astype('Int32')
passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
passenger_count_dim = passenger_count_dim[['passenger_count_id', 'passenger_count']]

df['RatecodeID'] = df['RatecodeID'].fillna(99).astype(int)
rate_code_type = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride",
    99: "Null/unknown"
}

rate_code_dim = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
rate_code_dim['rate_code_id'] = rate_code_dim.index
rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
rate_code_dim = rate_code_dim[['rate_code_id', 'RatecodeID', 'rate_code_name']]

payment_type_name = {
    0: "Flex Fare trip",
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip"
}

payment_type_dim = df[['payment_type']].drop_duplicates().reset_index(drop=True)
payment_type_dim['payment_type_id'] = payment_type_dim.index
payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)

payment_type_dim = payment_type_dim[['payment_type_id', 'payment_type', 'payment_type_name']]

vendor_type_name = {
    1:"Creative Mobile Technologies, LLC",
    2:"Curb Mobility, LLC",
    6:"Myle Technologies Inc",
    7:"Helix"
}

vendor_dim = df[['VendorID']].drop_duplicates().reset_index(drop=True)
vendor_dim['vendor_id'] = vendor_dim.index
vendor_dim['vendor_type_name'] = vendor_dim['VendorID'].map(vendor_type_name)
vendor_dim = vendor_dim[['vendor_id', 'VendorID', 'vendor_type_name']]

#Tabelas Fato
fact_table = df\
    .merge(vendor_dim, on='VendorID')\
    .merge(passenger_count_dim, on='passenger_count')\
    .merge(trip_distance_dim, on='trip_distance')\
    .merge(rate_code_dim, on='RatecodeID')\
    .merge(datetime_dim, on=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])\
    .merge(payment_type_dim, on='payment_type')\
    [['trip_id', 'vendor_id', 'datetime_id', 'passenger_count_id', 'trip_distance_id',
      'rate_code_id', 'payment_type_id', 'fare_amount', 'extra',
      'mta_tax', 'tip_amount', 'tolls_amount',
      'improvement_surcharge', 'total_amount']]

#Carregando os dados tratados na tabela silver
tables = {
    'datetime_dim': datetime_dim,
    'trip_distance_dim': trip_distance_dim,
    'passenger_count_dim': passenger_count_dim,
    'rate_code_dim': rate_code_dim,
    'payment_type_dim': payment_type_dim,
    'vendor_dim': vendor_dim,
    'fact_table': fact_table
}

s3 = boto3.client('s3')
for key, value in tables.items():
    buffer = BytesIO()
    value.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)

    s3.put_object(
    Bucket="uber-pipeline-2025",
    Key=f"silver/{key}/{key}.parquet",
    Body=buffer.getvalue()
)   