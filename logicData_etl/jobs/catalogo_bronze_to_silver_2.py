import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from transforms.cleaning import clean_catalogo_data
import json
import great_expectations as gx
from great_expectations.dataset import SparkDFDataset

def catalogo_bronze_to_silver():
    spark = SparkSession.builder.getOrCreate()
    args = getResolvedOptions(sys.argv, ['input_path', 'output_path', 'quarantine_path'])

    # 1. Lectura
    df_bronze = spark.read.option("header", "true").csv(args['input_path'])

    # 2. Limpieza y Validación de Calidad
    df_cleaned = clean_catalogo_data(df_bronze)
    
    # Great Expectations
    gx_df = SparkDFDataset(df_cleaned)
    gx_df.expect_column_values_to_not_be_null("id_producto")
    gx_df.expect_column_values_to_be_between("precio", min_value=0.01)
    
    validation_result = gx_df.validate()

    # 3. Separación de datos
    df_valid, df_failed = split_catalogo_data(df_cleaned)

    # 4. Escritura en S3
    if df_failed.count() > 0:
        df_failed.write.mode("append").parquet(f"{args['quarantine_path']}/data/")
    
    if df_valid.count() > 0:
        df_valid.write.format("delta").mode("overwrite").save(args['output_path'])
if __name__ == "__main__":
    catalogo_bronze_to_silver()