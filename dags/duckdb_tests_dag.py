
from datetime import datetime
from airflow import DAG
from astro import sql as aql
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
import pandas, duckdb, time, os

dag = DAG(
    dag_id="duckdb_tests_dag",
    start_date=datetime(2023, 4, 1),
    schedule=None,
    catchup=False,
)

MY_S3_BUCKET = 's3://jf-ml-data/flight_data/'
URL_PATH = f"{MY_S3_BUCKET}On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2022_1"

with dag:

    empty_task_1 = EmptyOperator(task_id='empty_task_1')
    empty_task_2 = EmptyOperator(task_id='empty_task_2')

    @task
    def csv_file_load_duckdb():
        duck_time = time.process_time()        
        conn = duckdb.connect()
        conn.sql("INSTALL httpfs")
        conn.sql("LOAD httpfs")
        conn.sql("SET s3_region='eu-central-1'")        
        conn.sql(f"CREATE TABLE duck_temp AS SELECT * FROM '{URL_PATH}.csv'")
        print(conn.sql("SELECT COUNT(*) FROM duck_temp").fetchone()[0])
        conn.close()
        duck_time = (time.process_time() - duck_time)
        print("DuckDB time: " + str(duck_time))
        return duck_time

    @task
    def csv_file_load_pandas():
        pandas_time = time.process_time()
        pandas_df = pandas.read_csv(f"{URL_PATH}.csv")
        print(len(pandas_df))
        pandas_time = (time.process_time() - pandas_time)      
        print("Pandas time: " + str(pandas_time))
        return pandas_time
    
    # flight_csv_to_parquet is a task that takes a year as an argument and loads all the csv 
    # files for that year into a parquet file using dcukdb.
    @task(max_active_tis_per_dag=1)
    def flight_csv_to_parquet(year):
        import uuid
        #You have to do this or else it will try to load the entire file into memory and your process will crash
        local_db_file = f'/tmp/{str(uuid.uuid1())}.db'
        conn = duckdb.connect(local_db_file)
        conn.sql("INSTALL httpfs")
        conn.sql("LOAD httpfs")
        conn.sql(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}'")
        conn.sql(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}'")
        conn.sql("SET s3_region='eu-central-1'")

        print(f"Reading year {year}")
        conn.sql(f"CREATE OR REPLACE TABLE duck_temp AS SELECT * FROM '{MY_S3_BUCKET}*{year}*.csv'")
        print(f"Year {year} has {conn.sql('SELECT COUNT(*) FROM duck_temp').fetchone()[0]} rows")
        
        print(f"Writing year {year}")
        conn.sql(f"COPY duck_temp TO '{MY_S3_BUCKET}flights.parquet' (FORMAT PARQUET, PARTITION_BY (YEAR, MONTH), ALLOW_OVERWRITE TRUE);")
        conn.close()
        os.remove(local_db_file)
        print("Done")
        return year

    ## Astro SDK section
    @aql.run_raw_sql(conn_id="duck_test")
    def create_files_processed_table():
        return """
        DROP TABLE IF EXISTS files_processed;
        CREATE TABLE files_processed (file_name VARCHAR);        
        """
    
    # If you don't set max_active_tis_per_dag to 1, you will get a "IO Error: Could not set lock on file" error. 
    @aql.run_raw_sql(conn_id="duck_test",max_active_tis_per_dag=1)
    def update_processed_file_list(file_name):
        return f"""
        INSERT INTO files_processed (file_name) VALUES ('{file_name}');
        """

    empty_task_1 >> [csv_file_load_duckdb(), csv_file_load_pandas()] >> empty_task_2 >> \
    flight_csv_to_parquet.expand(year=[2018,2019,2020,2021,2022]) >> \
    create_files_processed_table() >> \
    update_processed_file_list.expand(file_name=['2018_flights.csv','2019_flights.csv','2020_flights.csv','2021_flights.csv','2022_flights.csv'])
    

    




    
