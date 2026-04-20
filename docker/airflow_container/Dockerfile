# Usar la imagen base de Apache Airflow 2.8.3
FROM apache/airflow:2.8.3

# Cambiar a root para instalar las dependencias
USER root

# Actualizar el sistema y instalar herramientas necesarias
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    python3-dev \
    libpq-dev

# Limpiar la caché de apt para reducir el tamaño de la imagen
RUN rm -rf /var/lib/apt/lists/*

# Asegurarse de que pip esté actualizado
RUN python -m pip install --upgrade pip

# Cambiar al usuario airflow para instalar las librerías de Python necesarias
USER airflow

# Instalar la librería backports-datetime-fromisoformat
RUN pip install --user backports-datetime-fromisoformat

# Instalar las librerías de Python necesarias
RUN pip install --user sqlalchemy mysql-connector-python pandas psycopg2 dbt-postgres confluent_kafka kafka-python
RUN pip install apache-airflow-providers-amazon boto3

# Volver al usuario airflow para ejecutar la aplicación
USER airflow
