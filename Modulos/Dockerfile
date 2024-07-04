# Usa una imagen base de Python
FROM python:3.10-slim

# Instala Apache Airflow
RUN pip install apache-airflow

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /usr/src/app

# Copia los archivos necesarios al contenedor
COPY requirements.txt .
COPY main.py .
COPY Data_manage.py .

# Instala las dependencias del proyecto
RUN pip install --no-cache-dir -r requirements.txt

# Ejecuta el script como un comando predeterminado
CMD ["python", "./main.py"]
