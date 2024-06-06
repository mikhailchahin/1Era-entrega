import requests
import psycopg2
from psycopg2.extras import execute_values
import datetime

# Configuración de la API de OpenWeatherMap
api_key = 'YOUR_OPENWEATHERMAP_API_KEY'  # Reemplaza con tu API key
city = 'London'
url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'

# Configuración de Redshift
redshift_endpoint = 'your-redshift-cluster.endpoint'  # Reemplaza con tu endpoint
redshift_db = 'your-database-name'  # Reemplaza con tu nombre de base de datos
redshift_user = 'your-username'  # Reemplaza con tu usuario
redshift_password = 'your-password'  # Reemplaza con tu contraseña
redshift_port = 5439

# Extracción de datos desde la API
response = requests.get(url)
data = response.json()

# Transformación de los datos
weather_data = {
    'city': data['name'],
    'temperature': data['main']['temp'],
    'humidity': data['main']['humidity'],
    'weather': data['weather'][0]['description'],
    'timestamp': data['dt'],
    'ingestion_time': datetime.datetime.utcnow()
}

# Conexión a Redshift
conn = psycopg2.connect(
    dbname=redshift_db,
    user=redshift_user,
    password=redshift_password,
    host=redshift_endpoint,
    port=redshift_port
)

# Creación de la tabla en Redshift si no existe
create_table_query = """
CREATE TABLE IF NOT EXISTS weather_data (
    city VARCHAR(50),
    temperature FLOAT,
    humidity INT,
    weather VARCHAR(100),
    timestamp INT,
    ingestion_time TIMESTAMP
);
"""
with conn.cursor() as cursor:
    cursor.execute(create_table_query)
    conn.commit()

# Inserción de datos en Redshift
insert_query = """
INSERT INTO weather_data (city, temperature, humidity, weather, timestamp, ingestion_time)
VALUES %s;
"""

values = [
    (
        weather_data['city'],
        weather_data['temperature'],
        weather_data['humidity'],
        weather_data['weather'],
        weather_data['timestamp'],
        weather_data['ingestion_time']
    )
]

with conn.cursor() as cursor:
    execute_values(cursor, insert_query, values)
    conn.commit()

# Cerrar conexión
conn.close()

print("Datos insertados en Redshift correctamente.")
