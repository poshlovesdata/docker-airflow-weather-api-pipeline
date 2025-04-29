from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.test_scripts import action
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
# Load environment variables from .env file
WEATHER_API_KEY = os.getenv('WEATHER_API_KEY')


def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit

def transform_load_data(ti):
    data  = ti.xcom_pull(task_ids='extract_weather_data')


    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_farenheit= kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (F)": temp_farenheit,
                        "Feels Like (F)": feels_like_farenheit,
                        "Minimun Temp (F)":min_temp_farenheit,
                        "Maximum Temp (F)": max_temp_farenheit,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }

    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    # print(df_data)
    
    exports_dir = '/opt/airflow/output'
    
    # Ensure the directory exists
    os.makedirs(exports_dir, exist_ok=True)
    
    
    now = datetime.now()
    df_string = now.strftime("%Y-%m-%d %H:%M:%S")
    df_string = 'cuurrent_weather_data_' + df_string 
    filepath = os.path.join(exports_dir, f"{df_string}.csv")
    df_data.to_csv(filepath, index = False)  
    
    print("Data Saved to :", filepath)
    
    return filepath


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025,4,28),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    
}



with DAG(
    'weather_data_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False ) as dag:
    
    run_action = PythonOperator(
        task_id='run_action',
        python_callable=action,
    )
    
    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weather_api',
        endpoint=f'/data/2.5/weather?q=Lagos&appid={WEATHER_API_KEY}'
    )
    
    extract_weather_data = SimpleHttpOperator(
        task_id="extract_weather_data",
        http_conn_id="weather_api",
        method='GET',
        endpoint=f'/data/2.5/weather?q=Lagos&appid={WEATHER_API_KEY}',
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
    )
    
    transform_load_weather_data = PythonOperator(
        task_id = 'transform_load_weather_data',
        python_callable=transform_load_data,
        provide_context=True
    )
    
    
    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data >> run_action
    
    
    
        