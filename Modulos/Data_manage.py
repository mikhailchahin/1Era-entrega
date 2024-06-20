import psycopg2
from psycopg2.extras import execute_values
import logging

def connect_redshift(dbname, user, password, host, port):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        logging.info("Conexión a Redshift establecida correctamente.")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Error al conectar con Redshift: {e}")
        raise

def create_table(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS poke_datos (
        id INT PRIMARY KEY,
        name VARCHAR(50),
        height INT,
        weight INT,
        base_experience INT,
        ingestion_time TIMESTAMP
    );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()
        logging.info("Tabla 'poke_datos' verificada/creada en Redshift.")
    except psycopg2.Error as e:
        logging.error(f"Error al crear/verificar la tabla en Redshift: {e}")
        raise

def insert_data(conn, data):
    insert_query = """
    INSERT INTO poke_datos (id, name, height, weight, base_experience, ingestion_time)
    VALUES %s;
    """
    values = [
        (
            data['id'],
            data['name'],
            data['height'],
            data['weight'],
            data['base_experience'],
            data['ingestion_time']
        )
    ]
    try:
        with conn.cursor() as cursor:
            execute_values(cursor, insert_query, values)
            conn.commit()
        logging.info("Datos insertados en Redshift correctamente.")
    except psycopg2.Error as e:
        logging.error(f"Error al insertar datos en Redshift: {e}")
        raise
