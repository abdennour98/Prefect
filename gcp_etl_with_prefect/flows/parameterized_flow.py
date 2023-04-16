from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta
@task(retries=3,cache_key_fn=task_input_hash,cache_expiration=timedelta(days=1))
def fetch(dataset_url:str)->pd.DataFrame:
    """Read data from web  into pandas Dataframe"""
    df=pd.read_csv(dataset_url)
    return df
@task(log_prints=True)
def clean(df=pd.DataFrame)->pd.DataFrame:
    "    """"Fix dtypes issues"""
    df['tpep_pickup_datetime']=pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime']=pd.to_datetime(df['tpep_dropoff_datetime'])

    return df
@task()
def write_local(df:pd.DataFrame,color:str,data_file:str)->Path:
    """Write datafram outlocally as parquet file """
    path=Path(f'data/{color}/{data_file}')
    df.to_parquet(path,compression="gzip")
    return path
def write_gcs(path:str)->None:
    """upload local file parquet to gcs"""
    gcs_block = GcsBucket.load("gcs-zoomcamp")
    gcs_block.upload_from_path(
        from_path=f"{path}", to_path=path)

@flow()
def etl_web_to_gcs(color:str,year:int,month:str)->None:
    """The main ETL function"""
    dataset_file=f"{color}_tripdata_{year}-{month}"
    dataset_url=f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df=fetch(dataset_url)
    df_clean=clean(df)
    path=write_local(df_clean,color,dataset_file)
    write_gcs(path)
@flow()
def etl_parent_flow(months:list[str], year:int, color:str):
    """"run multiple flows"""   
    for month in months:
        etl_web_to_gcs(color,year,month)

if __name__ =='__main__':
    months=['03','04','05']
    year=2021
    color="yellow"
    etl_parent_flow(months, year, color)
