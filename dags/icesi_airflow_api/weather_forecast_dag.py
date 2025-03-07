import os
from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from dags.icesi_airflow_api.utils.weather_api import run_weather_forecast_pipeline
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Configuraciones básicas para la ejecución
URL = 'https://smn.conagua.gob.mx/tools/GUI/webservices/index.php?method=3'
SNOWFLAKE_CONN_ID = 'snowflake_conn_id'  # ID de conexión configurado en Airflow
DATABASE = 'DEV_ICESI'                   # Nombre de la base de datos en Snowflake
SCHEMA = 'SYSTEM_RECOMMENDATION'         # Nombre del esquema en Snowflake
TABLE = 'CONAGUA_WEATHER_RAW'            # Nombre de la tabla de destino en Snowflake
QUERIES_BASE_PATH = os.path.join(os.path.dirname(__file__), 'queries')

@dag(
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    default_args={'owner': 'airflow', 'retries': 1},
    tags=['weather', 'snowflake'],
    template_searchpath=QUERIES_BASE_PATH
)
def weather_forecast_dag():
    
    @task()
    def fetch_and_save_weather_data():
        # Ejecuta el pipeline para obtener los datos del clima y guardarlos en Snowflake
        run_weather_forecast_pipeline(
            url=URL,
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            database=DATABASE,
            schema=SCHEMA,
            table=TABLE
        )
    
    @task()
    def execute_snowflake_query():
        # Inicializamos el hook para conectarnos a Snowflake
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        # Leemos el contenido del archivo SQL "limpiar.sql" ubicado en la carpeta de queries
        sql_file_path = os.path.join(QUERIES_BASE_PATH, 'limpiar.sql')
        with open(sql_file_path, 'r') as file:
            sql_query = file.read()
        # Ejecutamos la consulta
        hook.run(sql_query)
    
    # Definimos el flujo de tareas: primero obtenemos y guardamos los datos y luego ejecutamos la consulta en Snowflake
    fetch_task = fetch_and_save_weather_data()
    execute_task = execute_snowflake_query()
    fetch_task >> execute_task

# Instanciamos el DAG
dag = weather_forecast_dag()