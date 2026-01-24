#!/usr/bin/env python
# coding: utf-8

# HOMEWORK: Script copiado do original e adequado para ler Parquet

# Este script foi convertido de um Jupyter Notebook através do comando:
# uv run jupyter nbconvert --to=script notebook.ipynb
import click
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')

#@click.option('--year', default=2021, type=int, help='Year of the data')
#@click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--year', default=2025, type=int, help='Year of the data')
@click.option('--month', default=11, type=int, help='Month of the data')

@click.option('--target-table', default='yellow_taxi_trips_2025_11', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')
#def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize):

@click.option('--input-file', default='/data/yellow_tripdata_2025-11.parquet', help='Caminho do arquivo Parquet dentro do container')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize, input_file):

    """Ingest NYC taxi data into PostgreSQL database."""

 # INICIO -- Direto do site oficial não foi possível, por isso o uso do arquivo localmente
 #    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
 #    url = f'{prefix}/yellow_tripdata_2021-01.csv.gz'
    prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
    url = f'{prefix}/yellow_tripdata_2025-11.parquet'
    
 #    url = input_file
 # FIM

    print(f"Lendo Parquet local: {input_file}")
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    print("Lendo arquivo Parquet em batches...")
    table = pq.read_table(input_file)

    print(f"Total de linhas no Parquet: {table.num_rows}")

    print("Gravando no PostgreSQL...")
    first = True
    for batch in table.to_batches(max_chunksize=chunksize):
        df = batch.to_pandas()

        df.to_sql(
            name=target_table,
            con=engine,
            if_exists='replace' if first else 'append',
            index=False
        )

        print(f"Inseridas {len(df)} linhas")
        first = False

    print("Ingest concluído com sucesso 🚀")

if __name__ == '__main__':
    run()