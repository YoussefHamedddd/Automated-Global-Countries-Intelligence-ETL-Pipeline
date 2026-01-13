
import requests
import pandas as pd


# def Extract():

#     url = "https://restcountries.com/v3.1/all?fields=name,capital,region,population,area"
#     headers = {'User-Agent': 'Mozilla/5.0'}
    
#     try:
#         response = requests.get(url, headers=headers)
#         response.raise_for_status()
#         data = response.json()
        
#         countries_list = []
#         for c in data:
#             countries_list.append({
#                 'name': c.get('name', {}).get('common'),
#                 'capital': c.get('capital', ['N/A'])[0] if c.get('capital') else 'N/A',
#                 'region': c.get('region'),
#                 'population': c.get('population', 0),
#                 'area': c.get('area', 0)
#             })
        
#         df = pd.DataFrame(countries_list)
#         df.to_csv('countries_temp.csv', index=False)
#     except Exception as e:

#         print(f": {e}")


# Extract();




df = pd.read_csv('countries_temp.csv')  

# print(df)

# print("number of missing rows in each coulmn")
# print(df.isnull().sum())


# # show counteis that dosent have any captial
# print(df[df['capital'].isnull()])



# Find rows where area is <= 0 OR population is < 0
bad_data = df[(df['area'] <= 0) | (df['population'] < 0)]
print("Rows with invalid Area or Population:")
print(bad_data)








# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.empty import EmptyOperator
# from datetime import datetime, timedelta
# import requests
# import pandas as pd
# import duckdb
# import psycopg2
# import os


# # --- Configuration ---
# ALERT_EMAIL = 'youssrfhamed1@example.com'

# def cleanup_temp_files():
#     print("Cleaning up old temporary files...")
#     files_to_delete = [
#         '/tmp/countries_temp.csv',
#         '/tmp/countries_final.csv'
#     ]
    
#     for file_path in files_to_delete:
#         if os.path.exists(file_path):
#             os.remove(file_path)
#             print(f"Deleted: {file_path}")
#         else:
#             print(f"File not found, skipping: {file_path}")

# def extract():
#     url = "https://restcountries.com/v3.1/all?fields=name,capital,region,population,area"
#     headers = {'User-Agent': 'Mozilla/5.0'}
#     response = requests.get(url, headers=headers)
#     response.raise_for_status()
#     data = response.json()
    
#     countries_list = [{
#         'name': c.get('name', {}).get('common'),
#         'capital': c.get('capital', [None])[0] if c.get('capital') else None,
#         'region': c.get('region'),
#         'population': c.get('population', 0),
#         'area': c.get('area', 0)
#     } for c in data]
    
#     df = pd.DataFrame(countries_list)
#     df.to_csv('/tmp/countries_temp.csv', index=False)

# def transform():
#     con = duckdb.connect()
#     con.execute("""
#         COPY (
#             SELECT 
#                 name, 
#                 COALESCE(capital, 'No Capital') AS capital, 
#                 region, 
#                 population, 
#                 area,
#                 (population / NULLIF(area, 0)) AS density
#             FROM '/tmp/countries_temp.csv'
#         ) TO '/tmp/countries_final.csv' (HEADER, DELIMITER ',');
#     """)



# def load():
#    def load():
#     print("Step 3: Loading data into PostgresSQL Database...")
#     conn = psycopg2.connect(
#         dbname="country_api", 
#         user="postgres", 
#         password="12345",
#         host="localhost",
#         port="5432"
#     )
  
#     cur = conn.cursor()
    

#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS country_metrics (
#             id SERIAL PRIMARY KEY,
#             name VARCHAR(255),
#             capital VARCHAR(255),
#             region VARCHAR(100),
#             population BIGINT,
#             area FLOAT,
#             density FLOAT
#         );
#     """)


#     cur.execute("TRUNCATE TABLE country_metrics;")

 
#     copy_sql = """
#         COPY country_metrics(name, capital, region, population, area, density)
#         FROM STDIN
#         WITH (FORMAT CSV, HEADER TRUE, QUOTE '"', DELIMITER ',');
#     """
    
#     with open('/tmp/countries_final.csv', 'r') as f:
#         cur.copy_expert(sql=copy_sql, file=f)
    
#     conn.commit()
#     cur.close()
#     conn.close()
#     print("Success: Data loaded successfully!")
    
# # --- DAG Definition ---
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2024, 1, 1),
#     'email': [ALERT_EMAIL],
#     'email_on_failure': False,  # Airflow handles the email if any task fails
#     'email_on_retry': False,
#     'retries': 0, 
# }

# with DAG(
#     dag_id='countries_etl',
#     default_args=default_args,
#     schedule='@once', 
#     catchup=False
# ) as dag:

#     start = EmptyOperator(task_id='start')

#     cleanup_task = PythonOperator(
#         task_id='cleanup_before_run',
#         python_callable=cleanup_temp_files
#     )

#     extract_task = PythonOperator(
#         task_id='extract_data',
#         python_callable=extract
#     )

#     transform_task = PythonOperator(
#         task_id='transform_data',
#         python_callable=transform
#     )

#     load_task = PythonOperator(
#         task_id='load_data',
#         python_callable=load
#     )

#     # Simplified Task Flow
#     start >> cleanup_task >> extract_task >> transform_task >> load_task