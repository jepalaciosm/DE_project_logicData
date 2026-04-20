import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ['input_path', 'output_path'])

input_path = args['input_path']
output_path = args['output_path']

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("id_producto", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("precio", StringType(), True),
    StructField("tipo_entrega", StringType(), True),
])

df = spark.read.option("header", "true").schema(schema).csv(input_path)

# Normalizar/validar precio: remover comas/espacios y convertir a double; filtrar no convertibles
df_clean = (
    df.withColumn("precio_clean", col("precio").cast(DoubleType()))
      .filter(col("precio_clean").isNotNull())
      .drop("precio")
      .withColumnRenamed("precio_clean", "precio")
)

# Opcional: mostrar conteos de valid/invalid
invalid_count = df.count() - df_clean.count()

df_clean.write.mode("overwrite").partitionBy("tipo_entrega").parquet(output_path)