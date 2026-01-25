#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import click
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype__green_tripdata = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "trip_type":"Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "ehail_fee":"float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
    "cbd_congestion_fee":"float64"
}

parse_dates__green_tripdata = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime"
]

dtype__taxi_zone_lookup = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string",
    "store_and_fwd_flag": "string"
}

def ingest_data_csv(
            url: str,
            engine,
            target_table: str,
            chunksize: int = 100000,
        ) -> pd.DataFrame:
    df_iter = pd.read_csv(
        url,
        dtype=dtype__taxi_zone_lookup,
        iterator=True,
        chunksize=chunksize
    )

    first = True

    for df_chunk in tqdm(df_iter):

        if first:
            # Create table schema (no data)
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace"
            )
            first = False
            print(f"Table {target_table} created")

        # Insert chunk
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )
        print(f"Inserted chunk: {len(df_chunk)}")

    print(f'Done ingesting to {target_table}')

def ingest_data_parquet(
            url: str,
            engine,
            target_table: str,
        ) -> pd.DataFrame:
    df = pd.read_parquet(
        url
    )

    # Configure data types
    df = df.astype(dtype__green_tripdata)

    for col in parse_dates__green_tripdata:
        df[col] = pd.to_datetime(df[col])

    # Create table schema (no data)
    df.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace"
    )
    print(f"Table {target_table} created")

    # Insert chunk
    df.to_sql(
        name=target_table,
        con=engine,
        if_exists="append"
    )
    print(f"Inserted chunk: {len(df)}")

    print(f'Done ingesting to {target_table}')

@click.command()
@click.option('--pg-user', default='root', show_default=True, help='Postgres user')
@click.option('--pg-pass', 'pg_pass', default='root', show_default=True, help='Postgres password')
@click.option('--pg-host', default='localhost', show_default=True, help='Postgres host')
@click.option('--pg-port', default='5432', show_default=True, help='Postgres port')
@click.option('--pg-db', default='ny_taxi', show_default=True, help='Postgres database')
# @click.option('--year', default=2021, show_default=True, type=int, help='Data year')
# @click.option('--month', default=1, show_default=True, type=int, help='Data month')
@click.option('--chunksize', default=100000, show_default=True, type=int, help='CSV read chunksize')
@click.option('--target-table', default='yellow_taxi_data', show_default=True, help='Target table name')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, chunksize, target_table):
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    url__taxi_zone_lookup = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
    url__green_tripdata = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'
    target__taxi_zone_lookup = 'taxi_zone_lookup'
    target__green_tripdata = 'green_tripdata'

    ingest_data_csv(
        url=url__taxi_zone_lookup,
        engine=engine,
        target_table=target__taxi_zone_lookup,
        chunksize=chunksize
    )

    ingest_data_parquet(
        url=url__green_tripdata,
        engine=engine,
        target_table=target__green_tripdata,
    )

if __name__ == '__main__':
    main()
