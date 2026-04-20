from pyspark.sql.functions import col, current_timestamp

def clean_catalogo_data(df):
    """Limpia el catálogo: elimina nulos en ID y añade timestamp."""
    return df.filter(col("id_producto").isNotNull()) \
             .withColumn("processed_at", current_timestamp())

def split_catalogo_data(df):
    """Separa datos válidos de los que rompen el contrato de negocio."""
    # Reglas lógicas (Deben coincidir con tus expectativas de GX)
    condicion_error = (
        (col("id_producto").isNull()) | 
        (col("precio").cast("float") <= 0) | 
        (~col("categoria").isin(["Electrónica", "Hogar", "Ropa", "Alimentos"]))
    )
    
    df_failed = df.filter(condicion_error)
    df_valid = df.subtract(df_failed)
    
    return df_valid, df_failed