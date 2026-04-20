import sys
import pathlib

# Asegurar que la raíz del repositorio está en sys.path para poder importar el paquete
ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """Crea una sesión de Spark local para pruebas unitarias."""
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("LogiData-Local-Tests") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    yield spark
    spark.stop()