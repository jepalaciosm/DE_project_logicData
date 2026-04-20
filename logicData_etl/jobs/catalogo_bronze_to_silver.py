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

def catalogo_bronze_to_silver():
    # 1. Iniciar Spark con soporte para Delta Lake
    # spark = SparkSession.builder \
    #     .appName("LogiData_Bronze_to_Silver") \
    #     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    #     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    #     .get_environment()
    spark = SparkSession.builder \
        .appName("LogiData_Bronze_to_Silver") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    args = getResolvedOptions(sys.argv, ['input_path', 'output_path','quarantine_path'])

    input_path = args['input_path']
    output_path = args['output_path']
    quarantine_path = args['quarantine_path']
    ts_string = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 2. Leer desde zona Bronze (S3)
    # bronze_path = "s3://logidata-bronze/raw_sensors/"
    
    # df_bronze = spark.read.format("parquet").load(bronze_path)

    schema = StructType([
        StructField("id_producto", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("precio", StringType(), True),
        StructField("tipo_entrega", StringType(), True),
    ])

    df_bronze = spark.read.option("header", "true").schema(schema).csv(input_path)

    # 3. Transformaciones Básicas (Limpieza)
    df_cleaned = df_bronze.filter(col("id_producto").isNotNull()) \
                          .withColumn("processed_at", current_timestamp())

    # 4. Validación de Calidad con Great Expectations (El Filtro)
    # Convertimos el DF de Spark a un objeto que GX entiende
    gx_df = SparkDFDataset(df_cleaned)

    # 3. Definición de Reglas de Calidad (Data Contract)
    # Validamos que id_producto no sea nulo
    gx_df.expect_column_values_to_not_be_null("id_producto")
    # Validamos que el precio sea positivo
    gx_df.expect_column_values_to_be_between("precio", min_value=0.01, max_value=10000.0)
    # Validamos categorías permitidas (opcional, muy útil para catálogos)
    gx_df.expect_column_values_to_be_in_set("categoria", ["Electrónicos","Ropa","Hogar","Perecederos","Medicamentos","Bebidas"])

    validation_result = gx_df.validate()

# 4. Lógica de Separación (Split)
    # Identificamos los registros que rompen las reglas
    df_failed = df_cleaned.filter(
        (col("id_producto").isNull()) | 
        (col("precio") <= 0) | 
        (~col("categoria").isin(["Electrónicos","Ropa","Hogar","Perecederos","Medicamentos","Bebidas"]))
    )


    df_valid = df_cleaned.subtract(df_failed)

    # 5. Gestión de Fallos (Cuarentena)
    if df_failed.count() > 0:
        print(f"⚠️ {df_failed.count()} productos fallaron las reglas de calidad.")
        
        # Guardar registros corruptos
        df_failed.withColumn("error_timestamp", current_timestamp()) \
                 .write.mode("append").parquet("s3://jepm-logicdata-dev-quarantine-067330477391/catalogo_errors/")
        # df_clean.write.mode("overwrite").partitionBy("tipo_entrega").parquet(output_path)
        # Guardar el reporte técnico del fallo
        try:
            report_json = json.dumps(validation_result.to_json_dict(), indent=2)
            spark.sparkContext.parallelize([report_json]) \
                .saveAsTextFile(f"quarantine_path__{ts_string}")
        except: 
            print("⚠️ No se logró guardar el reporte de validacion en bucket ")

    # 6. Carga a Silver (Capa Limpia)
    if df_valid.count() > 0:
        # df_valid.withColumn("fecha_carga", current_timestamp()) \
        #         .write.format("delta") \
        #         .mode("overwrite") \
        #         .save(output_path)
        # print("🚀 Capa Silver de Catálogo actualizada exitosamente.")
        # Añadimos la fecha de carga a los datos nuevos
        df_new_data = df_valid.withColumn("fecha_carga", current_timestamp())

        # Verificamos si la tabla ya existe para hacer MERGE, si no, creamos la tabla
        if DeltaTable.isDeltaTable(spark, output_path):
            print("🔄 Realizando MERGE en la tabla existente...")
            target_table = DeltaTable.forPath(spark, output_path)
            
            target_table.alias("target").merge(
                source = df_new_data.alias("source"),
                condition = "target.id_producto = source.id_producto" # Llave primaria
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
        else:
            print("🆕 La tabla no existe. Creando tabla Delta inicial...")
            df_new_data.write.format("delta").mode("overwrite").save(output_path)
            
        print("🚀 Capa Silver de Catálogo actualizada sin duplicados.")

if __name__ == "__main__":
    catalogo_bronze_to_silver()