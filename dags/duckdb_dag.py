
from datetime import datetime
from airflow import DAG, Dataset
from astro import sql as aql
from astro.sql.table import Table
from sqlalchemy import Column, Numeric, Date, String
from astro.files import get_file_list
from astro.files import File
from pandas import DataFrame
from airflow.decorators import task
import pandas
import os
import duckdb
import time


# DB_CONN_ID = os.environ["DB_CONN_ID"]
# FILE_CONN_ID = os.environ["FILE_CONN_ID"]
# BUCKET_NAME = os.environ["BUCKET_NAME"]

dag = DAG(
    dag_id="duckdb_demo_DAG",
    start_date=datetime(2023, 4, 1),
    schedule=None,
    catchup=False,
)

with dag:
    @task
    def csv_file_load_duckdb():
        duck_time = time.process_time()
        conn = duckdb.connect()
        conn.sql("INSTALL httpfs")
        conn.sql("LOAD httpfs")
        duck_df = conn.from_csv_auto("s3://jf-ml-data/flight_data/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2022_9.csv")
        conn.sql("create table duck_temp from select * from duck_df")
        conn.sql("select count(*) from duck_temp")
        #duck_df = conn.sql("SELECT count(*) FROM read_csv_auto('s3://jf-ml-data/flight_data/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2022_9.csv')").fetchone()
        print(duck_df)
        duck_time = (time.process_time() - duck_time)
        print(duck_time)
        return duck_time

    @task
    def csv_file_load_pandas():
        pandas_time = time.process_time()
        pandas_df = pandas.read_csv("s3://jf-ml-data/flight_data/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2022_9.csv")
        print(len(pandas_df))
        pandas_time = (time.process_time() - pandas_time)      
        print(pandas_time)
        return pandas_time

    csv_file_load_duckdb() >> csv_file_load_pandas()