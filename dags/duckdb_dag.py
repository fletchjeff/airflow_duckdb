
from datetime import datetime
from airflow import DAG
from astro import sql as aql
from airflow.decorators import task
import pandas, os, duckdb, time, glob

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
        conn.sql("CREATE TABLE duck_temp AS SELECT * FROM 's3://jf-ml-data/flight_data/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2022_1.csv'")
        print(conn.sql("SELECT COUNT(*) FROM duck_temp").fetchone()[0])
        duck_time = (time.process_time() - duck_time)
        print("DuckDB time: " + str(duck_time))
        return duck_time

    @task
    def csv_file_load_pandas():
        pandas_time = time.process_time()
        pandas_df = pandas.read_csv("s3://jf-ml-data/flight_data/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2022_1.csv")
        print(len(pandas_df))
        pandas_time = (time.process_time() - pandas_time)      
        print("Pandas time: " + str(pandas_time))
        return pandas_time
    
    @task
    def load_multiple_csv_files_duckdb():
        duck_time = time.process_time()
        conn = duckdb.connect()
        conn.sql("INSTALL httpfs")
        conn.sql("LOAD httpfs")
        print("loading initial file s3://jf-ml-data/flight_data/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2022_1.csv")
        duck_df = conn.from_csv_auto("s3://jf-ml-data/flight_data/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2022_1.csv")
        conn.sql("CREATE TABLE duck_temp AS SELECT * FROM duck_df")
        for i in range(2, 13):
            print("loading file s3://jf-ml-data/flight_data/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2022_" + str(i) + ".csv")
            conn.sql("COPY duck_temp from 's3://jf-ml-data/flight_data/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2022_" + str(i) + ".csv' (HEADER TRUE, DELIMITER ',')")
        print(conn.sql("SELECT COUNT(*) FROM duck_temp").fetchone()[0])
        conn.sql("COPY duck_temp TO 'include/tmp/all_flights.parquet' (FORMAT PARQUET);")
        duck_time = (time.process_time() - duck_time)
        print("DuckDB time: " + str(duck_time))
        return duck_time
    
    @task
    def get_top_10_destination_cities(parquet_path):
        conn = duckdb.connect()
        top_10_destination_cities_query = conn.sql(f"select DestCityName,count(DestCityName) as count from '{parquet_path}'parquet_path group by DestCityName order by count desc limit 10").fetchall()
        top_10_destination_cities = []
        for city in top_10_destination_cities_query:
            top_10_destination_cities.append(city[0])
        return top_10_destination_cities
    
    @task
    def filter_top_cities(parquet_path, destination_city):
        duck_time = time.process_time()
        conn = duckdb.connect()
        duck_df = conn.sql(f"SELECT * FROM '{parquet_path}' WHERE DestCityName='{destination_city}'")
        conn.sql("CREATE TABLE duck_temp AS SELECT * FROM duck_df")
        conn.sql(f"COPY duck_temp TO 'include/tmp/flights_{destination_city.replace(',','').replace(' ','_').replace('/','_')}.parquet' (FORMAT PARQUET);")
        duck_time = (time.process_time() - duck_time)
        return duck_time
    
    @task
    def large_file_load_duckdb():
        duck_time = time.process_time()

        #You have to do this or else it will try to load the entire file into memory and your process will crash
        conn = duckdb.connect('/tmp/large_file_load.db')
        conn.sql("INSTALL httpfs")
        conn.sql("LOAD httpfs")
        conn.sql("CREATE TABLE duck_temp AS SELECT * FROM read_parquet('s3://jf-ml-data/all_flight_data.parquet')")
        print(conn.sql("SELECT COUNT(*) FROM duck_temp").fetchone()[0])
        duck_time = (time.process_time() - duck_time)
        return duck_time

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
    top_cities_list = get_top_10_destination_cities('include/tmp/all_flights.parquet')
    [csv_file_load_duckdb(), csv_file_load_pandas()] >> \
    load_multiple_csv_files_duckdb() >> \
    top_cities_list >> \
    filter_top_cities.partial(parquet_path='include/tmp/all_flights.parquet').expand(destination_city=top_cities_list) >> \
    large_file_load_duckdb() >> \
    create_files_processed_table() >> \
    update_processed_file_list.expand(file_name=glob.glob("include/tmp/flights_*.parquet"))
    
    # load_multiple_csv_files_duckdb()

    




    
