import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, dayofweek
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import json
from datetime import datetime
from delta.tables import DeltaTable
from typing import Dict, List, Any



def silver_to_gold_operation_summary():
    # 1. Iniciar Spark con soporte para Delta Lake
    # función de transformación para consolidar la operación
    spark = SparkSession.builder \
        .appName(f"LogiData_Silver_to_Gold") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    args = getResolvedOptions(sys.argv, ['silver_path', 'gold_path', 'quarantine_path'])

    silver_path = args['silver_path']
    gold_path = args['gold_path']
    quarantine_path = args['quarantine_path']
    ts_string = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Cargar tablas Silver
    pedidos = spark.read.format("delta").load(f"{silver_path}/pedidos/")
    clientes = spark.read.format("delta").load(f"{silver_path}/clientes/")
    entregas = spark.read.format("delta").load(f"{silver_path}/entregas/")

    # Transformación: Unir todo para el negocio
    gold_df = pedidos.alias("p") \
        .join(clientes.alias("c"), "id_cliente", "inner") \
        .join(entregas.alias("e"), "id_pedido", "left") \
        .select(
            "p.id_pedido",
            "c.nombre",
            "c.zona",
            "p.monto",
            "p.estado",
            "e.conductor",
            "p.fecha"
        )

    # Guardar en Gold
    gold_df.write.format("delta").mode("overwrite").save(f"{gold_path}/resumen_operativo/")

def silver_to_gold_demand_prediction():
    """Esta tabla servirá para dos propósitos: 
    * alimentar tableros de control y servir como base para entrenar modelos que predigan la demanda por zona"""
    spark = SparkSession.builder \
        .appName(f"LogiData_Silver_to_Gold_prediccion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    args = getResolvedOptions(sys.argv, ['silver_path', 'gold_path', 'quarantine_path'])

    silver_path = args['silver_path']
    gold_path = args['gold_path']
    
    # 1. Carga de tablas Silver
    df_pedidos = spark.read.format("delta").load(f"{silver_path}/pedidos/")
    df_clientes = spark.read.format("delta").load(f"{silver_path}/clientes/")
    df_catalogo = spark.read.format("delta").load(f"{silver_path}/catalogo/")
    df_entregas = spark.read.format("delta").load(f"{silver_path}/entregas/")

    # 2. Construcción del Dataset Maestro (Joins)
    # Unimos pedidos con clientes y catálogo para tener el contexto completo
    master_df = df_pedidos.alias("p") \
        .join(df_clientes.alias("c"), "id_cliente", "inner") \
        .join(df_catalogo.alias("cat"), "id_producto", "inner") \
        .join(df_entregas.alias("e"), "id_pedido", "left")

    # 3. Ingeniería de Características (Features)
    final_gold = master_df.select(
        "p.id_pedido",
        "p.fecha",
        "c.zona",
        "c.tipo_cliente",
        "cat.categoria",
        "p.monto",
        "e.conductor"
    ).withColumn("es_fin_de_semana", (dayofweek(col("fecha")).isin([1, 7])))

    # 4. Escritura en Gold con Sobrescritura Controlada
    final_gold.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(f"{gold_path}/master_demanda_prediccion/")

    print("🚀 Tabla Gold para Predicción de Demanda generada exitosamente.")
        

if __name__ == "__main__":
    silver_to_gold_operation_summary()
    silver_to_gold_demand_prediction()
    