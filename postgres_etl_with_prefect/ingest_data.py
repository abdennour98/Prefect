import pandas as pd
import argparse
from sqlalchemy import create_engine
import os
from time import time
from prefect import flow,task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector


@task(log_prints=True,retries=3,cache_key_fn=task_input_hash,cache_expiration=timedelta(days=1))
def extract_data(url):
    parquet_name="output.parquet"
    csv_name="output.csv"

    os.system(f"wget {url} -O {parquet_name}")
    pd.read_parquet(parquet_name).to_csv(csv_name)
    df_iter=pd.read_csv(csv_name,index_col=0,iterator=True,chunksize=100000)
    df=next(df_iter)
    df.tpep_pickup_datetime=pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime=pd.to_datetime(df.tpep_dropoff_datetime)
    return df
@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df=df[df['passenger_count']!=0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df
@task(log_prints=True,retries=3)
def ingest_data(table_name,raw_data):
   
    df=raw_data
    connectionbock=SqlAlchemyConnector.load("postgresconnection")
    with connectionbock.get_connection(begin=False) as engine:

        df.head(n=0).to_sql(name=table_name,con=engine,if_exists='replace')
   
            
        df.to_sql(name=table_name,con=engine,if_exists='append')
        print("Data is inserted successfully")
@flow(name="Subflow")
def log_subflow(table_name:str):
    print(f"logging subflow for the table name is {table_name}")
@flow(name="Ingest Flow")
def main_flow(table_name:str):
    table_name=table_name
    url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
    log_subflow(table_name)
    raw_data=extract_data(url)
    data=transform_data(raw_data)
    ingest_data(user,password,host,port,db,table_name,data)
      

if __name__=='__main__':
     main_flow("yellow_taxi_trips")  
      

     
    






