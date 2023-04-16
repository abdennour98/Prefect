from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
@task(retries=3)
def extract_from_gcs(color:str,year:int,month:str)->Path:
    """Download trip data from gcs """
    gcs_path=f"data/{color}/{color}_tripdata_{year}-{month}"
    gcs_block = GcsBucket.load("gcs-zoomcamp")
    gcs_block=gcs_block.get_directory(from_path=gcs_path, local_path=f"gcs_data/")
    return Path(f"gcs_data/{gcs_path}")
@task
def transform_data(path:Path)->pd.DataFrame:
    """transfom data before load it in big query"""
    df=pd.read_parquet(path)
    print(f"pre:messing passengers {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0,inplace=True)
    print(f"post:messing passengers {df['passenger_count'].isna().sum()}")
    return df
def write_data_in_bq(df:pd.DataFrame)->None:
    """"Load dataframe to big query table"""
    gcp_credentials_block = GcpCredentials.load("gcpcredentials")
    df.to_gbq(
        destination_table="trip_data.rides",
         project_id="dtc-de-380810",
         credentials=gcp_credentials_block.get_credentials_from_service_account(),
         chunksize=100000,
         if_exists="append"
    )
@flow()
def etl_gcs_to_bq():
    """This is the main etl flow to load data from google storage into big query"""
    color="yellow"
    year=2021
    month="01"
    path=extract_from_gcs(color,year,month)
    df=transform_data(path)
    write_data_in_bq(df)




if __name__=="__main__":
    etl_gcs_to_bq()
