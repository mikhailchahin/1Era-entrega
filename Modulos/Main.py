import requests
import datetime
import logging
from Data_manage import connect_redshift, create_table, insert_data

# Configuración del logging
logging.basicConfig(
    filename='applog.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuración de la API de PokeAPI
api_url = 'https://pokeapi.co/api/v2/pokemon/1'

# Configuración de Redshift
redshift_endpoint = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
redshift_db = 'data-engineer-database'
redshift_user = 'acevedomikhail_lv_coderhouse'
redshift_password = 'Bs1H2V5S4C'
redshift_port = 5439

try:
    # Extracción de datos desde la API de PokeAPI
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()
    logging.info(f"Datos extraídos de la API: {data}")

    # Transformación de los datos
    pokemon_data = {
        'id': data['id'],
        'name': data['name'],
        'height': data['height'],
        'weight': data['weight'],
        'base_experience': data['base_experience'],
        'ingestion_time': datetime.datetime.utcnow()
    }
    logging.info(f"Datos transformados: {pokemon_data}")

    # Conexión a Redshift
    conn = connect_redshift(redshift_db, redshift_user, redshift_password, redshift_endpoint, redshift_port)

    # Creación de la tabla en Redshift si no existe
    create_table(conn)

    # Inserción de datos en Redshift
    insert_data(conn, pokemon_data)

except requests.exceptions.RequestException as e:
    logging.error(f"Error al realizar la petición a la API: {e}")
except psycopg2.Error as e:
    logging.error(f"Error al interactuar con Redshift: {e}")
except Exception as e:
    logging.error(f"Ocurrió un error inesperado: {e}")
finally:
    if 'conn' in locals() and conn:
        conn.close()
        logging.info("Conexión a Redshift cerrada.")
