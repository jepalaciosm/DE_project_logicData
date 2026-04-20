import time
import json
import psycopg2
from kafka import KafkaProducer

# Configuración de conexión a PostgreSQL
host = 'host.docker.internal'  # Cambia si tu PostgreSQL está en otro host
port = '5434'  # Puerto por defecto de PostgreSQL
dbname = 'postgres'  # Nombre de la base de datos
user = 'admin'  # Tu usuario de PostgreSQL
password = 'root'  # Tu contraseña de PostgreSQL

# Configuración de Kafka
kafka_host = 'host.docker.internal:9092'  # Dirección de tu broker Kafka
kafka_topic = 'medicamentos'  # Nombre del tópico en Kafka

# Conectar con el servidor Kafka
producer = KafkaProducer(
    bootstrap_servers=[kafka_host],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialización de datos a JSON
)

# Conectar con la base de datos PostgreSQL
connection = psycopg2.connect(
    host=host,
    port=port,
    dbname=dbname,
    user=user,
    password=password
)
cursor = connection.cursor()

# Inicializamos el offset en 0
offset = 0

# Bucle para procesar los datos
while True:
    try:
        # Realizar una consulta SQL con OFFSET y LIMIT para obtener 2 registros
        query = f"""
            SELECT principio_activo, unidad_de_dispensacion, concentracion, unidad_base, nombre_comercial, fabricante, precio_por_tableta, factoresprecio, numerofactor
            FROM medicamentos
            LIMIT 500 OFFSET {offset};
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        print("consulta realizada")
        print(query)
        if not rows:
            print("No hay más registros en la base de datos.")
            break  # No hay más datos para enviar, salir del bucle

        # Iterar sobre los resultados y enviarlos a Kafka
        for row in rows:
            data = {
                'principio_activo': row[0],
                'unidad_de_dispensacion': row[1],
                'concentracion': row[2],
                'unidad_base': row[3],
                'nombre_comercial': row[4],  # Convertir la fecha a formato string
                'fabricante': row[5],
                'precio_por_tableta': row[6],
                'factoresprecio': row[7],
                'numerofactor': row[8]
            }

            # Enviar el mensaje a Kafka
            producer.send(kafka_topic, value=data)
            print(f"Enviado a Kafka: {data}")

        # Incrementamos el offset para el siguiente bloque de 2 registros
        offset += 500
        producer.flush()
        # Esperar 5 segundos antes de enviar los siguientes datos
        time.sleep(5)

    except Exception as e:
        print(f"Error al enviar los datos: {e}")
        break

# Cerrar la conexión a la base de datos y al productor de Kafka
cursor.close()
connection.close()
producer.close()

