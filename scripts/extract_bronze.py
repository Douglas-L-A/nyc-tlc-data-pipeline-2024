import pandas as pd
import boto3
from io import BytesIO

df = pd.read_parquet(r"data/yellow_tripdata_2024.parquet")

#Particionando os dados por data
df['pickup_date'] = df['tpep_pickup_datetime'].dt.date
df_dia = {
    dia.strftime('%Y-%m-%d'): grupo for dia, grupo in df.groupby('pickup_date')
}

#Carregando na camada bronze (AWS S3)
s3 = boto3.client('s3')
buffer = BytesIO()
df.to_parquet(buffer, index=False, compression='snappy')
buffer.seek(0)

s3.put_object(
    Bucket="uber-pipeline-2024",
    Key=f"bronze//yellow_tripdata_2024.parquet",
    Body=buffer.getvalue()
)

for key, value in df_dia.items():
    buffer = BytesIO()
    value.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)

    ano, mes, dia = key.split('-')
    s3.put_object(
        Bucket="uber-pipeline-2024",
        Key=f"bronze/{ano}/{mes}/{dia}/{key}.parquet",
        Body=buffer.getvalue()
    )   