from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
@task(retries=3)
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
def etl_web_to_gcs()->None:
    """The main ETL function"""
    color="yellow"
    year=2021
    month=1
    dataset_file=f"{color}_tripdata_{year}-{month:02}"
    dataset_url=f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df=fetch(dataset_url)
    df_clean=clean(df)
    path=write_local(df_clean,color,dataset_file)
    write_gcs(path)
if __name__ =='__main__':
    etl_web_to_gcs()