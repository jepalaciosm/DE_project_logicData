import pytest
from logicData_etl.transforms.cleaning import clean_catalogo_data

def test_clean_catalogo_data(spark_session):
    # 1. Crear datos de prueba (Mocks)
    data = [("P001", "10.5"), (None, "20.0")]
    df = spark_session.createDataFrame(data, ["id_producto", "precio"])
    
    # 2. Ejecutar transformación
    result_df = clean_catalogo_data(df)
    
    # 3. Validaciones (Assertions)
    assert result_df.count() == 1
    assert "processed_at" in result_df.columns