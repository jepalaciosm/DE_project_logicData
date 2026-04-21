import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import json
import great_expectations as gx
from great_expectations.dataset import SparkDFDataset
from datetime import datetime
from delta.tables import DeltaTable
from typing import Dict, List, Any

# ============================================================================
# CONFIGURACIÓN DE ENTIDADES
# ============================================================================

ENTITY_CONFIGS = {
    "catalogo": {
        "entity_name": "Catálogo de Productos",
        "schema": StructType([
            StructField("id_producto", StringType(), True),
            StructField("categoria", StringType(), True),
            StructField("precio", StringType(), True),
            StructField("tipo_entrega", StringType(), True),
        ]),
        "primary_key": "id_producto",
        "null_check_fields": ["id_producto"],
        "enum_fields": {
            "categoria": ["Electrónicos", "Ropa", "Hogar", "Perecederos", "Medicamentos", "Bebidas"]
        },
        "range_fields": {
            "precio": {"min": 0.01, "max": 10000.0}
        }
    },
    "clientes": {
        "entity_name": "Clientes",
        "schema": StructType([
            StructField("id_cliente", StringType(), True),
            StructField("nombre", StringType(), True),
            StructField("zona", StringType(), True),
            StructField("tipo_cliente", StringType(), True),
        ]),
        "primary_key": "id_cliente",
        "null_check_fields": ["id_cliente", "nombre"],
        "enum_fields": {},
        "range_fields": {}
    }
}

# ============================================================================
# FUNCIÓN GENÉRICA
# ============================================================================

def bronze_to_silver(entity_type: str):
    """
    Función genérica para procesar datos de Bronze a Silver.
    
    Args:
        entity_type: Tipo de entidad ('catalogo' o 'clientes')
    """
    
    # Validar que la entidad esté configurada
    if entity_type not in ENTITY_CONFIGS:
        raise ValueError(f"Entidad '{entity_type}' no configurada. Disponibles: {list(ENTITY_CONFIGS.keys())}")
    
    config = ENTITY_CONFIGS[entity_type]
    
    # 1. Iniciar Spark con soporte para Delta Lake
    spark = SparkSession.builder \
        .appName(f"LogiData_{entity_type.capitalize()}_Bronze_to_Silver") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    args = getResolvedOptions(sys.argv, ['input_path', 'output_path', 'quarantine_path'])

    input_path = args['input_path']
    output_path = args['output_path']
    quarantine_path = args['quarantine_path']
    ts_string = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 2. Leer desde zona Bronze (S3)
    # Determinar tipo de entidad y ajustar rutas
      
    input_path = f"{input_path}/{entity_type}/"
    output_path = f"{output_path}/{entity_type}/"
    quarantine_path = f"{quarantine_path}/{entity_type}/"
    ts_string = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 2. Leer desde zona Bronze (S3)
    df_bronze = spark.read.option("header", "true").schema(config["schema"]).csv(f"{input_path}/{entity_type}.csv")

    # 3. Transformaciones Básicas (Limpieza)
    primary_key = config["primary_key"]
    df_cleaned = df_bronze.withColumn("processed_at", current_timestamp())

    # 4. Validación de Calidad con Great Expectations
    gx_df = SparkDFDataset(df_cleaned)
    
    # Aplicar validaciones dinámicas según configuración
    _apply_validations(gx_df, config)
    
    validation_result = gx_df.validate()

    # 5. Lógica de Separación (Identificar registros inválidos)
    df_failed = _filter_invalid_records(df_cleaned, config)
    df_valid = df_cleaned.subtract(df_failed)

    # 6. Gestión de Fallos (Cuarentena)
    if df_failed.count() > 0:
        print(f"⚠️ {df_failed.count()} registros de {config['entity_name']} fallaron las reglas de calidad.")
        
        # Guardar registros corruptos
        df_failed.withColumn("error_timestamp", current_timestamp()) \
                 .write.mode("append").parquet(f"{quarantine_path}/{entity_type}_errors/")
        
        # Guardar el reporte técnico del fallo
        try:
            report_json = json.dumps(validation_result.to_json_dict(), indent=2)
            spark.sparkContext.parallelize([report_json]) \
                .saveAsTextFile(f"{quarantine_path}/{entity_type}_reports__{ts_string}")
        except: 
            print("⚠️ No se logró guardar el reporte de validación en bucket")

    # 7. Carga a Silver (Capa Limpia)
    if df_valid.count() > 0:
        df_new_data = df_valid.withColumn("fecha_carga", current_timestamp())

        # Mergear o crear tabla Delta
        if DeltaTable.isDeltaTable(spark, output_path):
            print("🔄 Realizando MERGE en la tabla existente...")
            target_table = DeltaTable.forPath(spark, output_path)
            
            target_table.alias("target").merge(
                source = df_new_data.alias("source"),
                condition = f"target.{primary_key} = source.{primary_key}"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
        else:
            print(f"🆕 La tabla no existe. Creando tabla Delta inicial...")
            df_new_data.write.format("delta").mode("overwrite").save(output_path)
            
        print(f"🚀 Capa Silver de {config['entity_name']} actualizada sin duplicados.")
    else:
        print(f"⚠️ No hay registros válidos para procesar en {config['entity_name']}")


def _apply_validations(gx_df, config: Dict[str, Any]):
    """
    Aplica validaciones dinámicas según la configuración de la entidad.
    """
    # Validaciones de nulidad
    for field in config["null_check_fields"]:
        gx_df.expect_column_values_to_not_be_null(field)
    
    # Validaciones de valores permitidos (enums)
    for field, allowed_values in config["enum_fields"].items():
        gx_df.expect_column_values_to_be_in_set(field, allowed_values)
    
    # Validaciones de rango
    for field, range_config in config["range_fields"].items():
        gx_df.expect_column_values_to_be_between(
            field, 
            min_value=range_config["min"], 
            max_value=range_config["max"]
        )


def _filter_invalid_records(df, config: Dict[str, Any]):
    """
    Filtra registros que no cumplen las reglas de calidad configuradas.
    """
    conditions = []
    
    # Condiciones de nulidad
    for field in config["null_check_fields"]:
        conditions.append(col(field).isNull())
    
    # Condiciones de valores permitidos
    for field, allowed_values in config["enum_fields"].items():
        conditions.append(~col(field).isin(allowed_values))
    
    # Condiciones de rango
    for field, range_config in config["range_fields"].items():
        conditions.append(
            (col(field) < range_config["min"]) | (col(field) > range_config["max"])
        )
    
    # Combinar todas las condiciones con OR
    if conditions:
        filter_condition = conditions[0]
        for cond in conditions[1:]:
            filter_condition = filter_condition | cond
        return df.filter(filter_condition)
    
    return df.limit(0)  # Retorna DF vacío si no hay condiciones


# ============================================================================
# FUNCIONES ESPECÍFICAS PARA CADA ENTIDAD
# ============================================================================

def catalogo_bronze_to_silver():
    """Procesa catálogo de productos de Bronze a Silver."""
    bronze_to_silver("catalogo")


def clientes_bronze_to_silver():
    """Procesa datos de clientes de Bronze a Silver."""
    bronze_to_silver("clientes")


if __name__ == "__main__":
    # Determinar qué entidad procesar según parámetro
    # Por defecto: catalogo
    # entity = sys.argv[1] if len(sys.argv) > 4 else "catalogo"
    # bronze_to_silver(entity)
    catalogo_bronze_to_silver()
    clientes_bronze_to_silver()