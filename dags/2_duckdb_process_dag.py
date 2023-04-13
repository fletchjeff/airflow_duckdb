
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
import pandas, duckdb, os

dag = DAG(
    dag_id="2_duckdb_process_dag",
    start_date=datetime(2023, 4, 1),
    schedule=None,
    catchup=False
)

FILE_URL = '/tmp/all_flights.db'
MY_S3_BUCKET = 's3://jf-ml-data/flight_data/'

with dag:
    
    @task
    def flight_parquet_to_local_db_file(local_file_path):
        conn = duckdb.connect(local_file_path)
        conn.sql("INSTALL httpfs")
        conn.sql("LOAD httpfs")
        conn.sql(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}'")
        conn.sql(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}'")
        conn.sql("SET s3_region='eu-central-1'")
        conn.sql(f"CREATE TABLE IF NOT EXISTS all_flights AS SELECT * FROM '{MY_S3_BUCKET}/flights.parquet/*/*/*.parquet'")
        print(conn.sql("SELECT COUNT(*) FROM all_flights").fetchone()[0])
        return local_file_path

    @task
    def get_top_10_destination_cities(local_file_path):
        conn = duckdb.connect(local_file_path,read_only=True)
        top_10_destination_cities_query = conn.sql(
            f"""SELECT DestCityName,count(DestCityName) 
                AS count FROM all_flights 
                group by DestCityName order by count desc limit 10""").fetchall()
        top_10_destination_cities = []
        for city in top_10_destination_cities_query:
            top_10_destination_cities.append(city[0])
        return top_10_destination_cities

    @task
    def filter_top_cities(local_file_path, destination_city):
        conn = duckdb.connect(local_file_path,read_only=True)
        city_flight_count = conn.sql(f"SELECT count(*) FROM all_flights WHERE DestCityName='{destination_city}'").fetchone()[0]
        return {'city_name' : destination_city, "flight_count" : city_flight_count}
    
    @task
    def store_top_cities(top_cities):
        cities_df = pandas.DataFrame.from_dict(top_cities)
        conn = duckdb.connect()
        conn.sql("INSTALL httpfs")
        conn.sql("LOAD httpfs")
        conn.sql(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}'")
        conn.sql(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}'")
        conn.sql("SET s3_region='eu-central-1'")        
        conn.sql("CREATE TABLE cities_table AS SELECT * FROM cities_df")
        conn.sql("COPY cities_table TO 's3://jf-ml-data/cities_table.parquet' (FORMAT PARQUET);")
        return top_cities

    local_file_path = flight_parquet_to_local_db_file(FILE_URL)
    top_cities_list = get_top_10_destination_cities(local_file_path=local_file_path)
    top_cities_count = filter_top_cities.partial(local_file_path=local_file_path).expand(destination_city=top_cities_list)
    store_top_cities(top_cities_count)
    
