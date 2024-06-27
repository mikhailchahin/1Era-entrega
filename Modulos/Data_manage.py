# data_manage.py

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

def create_table(conn, table_name):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
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
        logging.info(f"Tabla '{table_name}' verificada/creada en Redshift.")
    except psycopg2.Error as e:
        logging.error(f"Error al crear/verificar la tabla en Redshift: {e}")
        raise

def insert_data(conn, data, table_name):
    insert_query = f"""
    INSERT INTO {table_name} (id, name, height, weight, base_experience, ingestion_time)
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
        logging.info(f"Datos insertados en Redshift correctamente en tabla '{table_name}'.")
    except psycopg2.Error as e:
        logging.error(f"Error al insertar datos en Redshift: {e}")
        raise
