resource "aws_glue_job" "catalogo_etl" {
  name     = "${var.project}-${var.env}-catalogo-etl"
  role_arn = var.glue_role_arn

  command {
    script_location = var.script_location
    python_version  = "3"
  }


  default_arguments = {
    "--input_path"  = "s3://${var.bronze_bucket}/"
    "--output_path" = "s3://${var.silver_bucket}/"
    "--TempDir"     = "s3://${var.temp_bucket}/temp/"
  }


  worker_type       = "G.1X"
  number_of_workers = 2

  glue_version = "5.0"

  timeout = 10

  tags = var.tags
}

resource "aws_glue_job" "catalogo_etl_broze_silver" {
  name     = "${var.project}-${var.env}-catalogo-etl-broze-silver"
  role_arn = var.glue_role_arn

  command {
    script_location = var.script_catalogo_etl_broze_silver
    python_version  = "3"
  }


  default_arguments = {
    "--input_path"  = "s3://${var.bronze_bucket}/"
    "--output_path" = "s3://${var.silver_bucket}/"
    "--silver_path" = "s3://${var.silver_bucket}/"
    "--gold_path" = "s3://${var.gold_bucket}/"
    "--quarantine_path" = "s3://${var.quarantine_bucket}/"
    "--TempDir"     = "s3://${var.temp_bucket}/temp/"
    # --- INSTALACIÓN DE GREAT EXPECTATIONS ---
    # Se recomienda forzar typing_extensions para evitar conflictos de versiones en Glue
    "--additional-python-modules" = "great_expectations==0.17.23,typing_extensions>=4.5.0"
    "--extra-py-files" = "s3://${var.temp_bucket}/temp/"
    # Habilita el logueo continuo para ver errores de instalación en CloudWatch
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--datalake-formats" = "delta"  
  }


  worker_type       = "G.1X"
  number_of_workers = 2

  glue_version = "5.0"

  timeout = 10

  tags = var.tags
}

resource "aws_glue_job" "etl_silver_gold" {
  name     = "${var.project}-${var.env}-etl-silver-gold"
  role_arn = var.glue_role_arn

  command {
    script_location = var.script_etl_silver_gold
    python_version  = "3"
  }


  default_arguments = {
    "--input_path"  = "s3://${var.bronze_bucket}/"
    "--output_path" = "s3://${var.silver_bucket}/"
    "--silver_path" = "s3://${var.silver_bucket}/"
    "--gold_path" = "s3://${var.gold_bucket}/"
    "--quarantine_path" = "s3://${var.quarantine_bucket}/"
    "--TempDir"     = "s3://${var.temp_bucket}/temp/"
    "--extra-py-files" = "s3://${var.temp_bucket}/temp/"
    # Habilita el logueo continuo para ver errores de instalación en CloudWatch
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--datalake-formats" = "delta"  
  }


  worker_type       = "G.1X"
  number_of_workers = 2

  glue_version = "5.0"

  timeout = 10

  tags = var.tags
}

# Crear la base de datos en el Catálogo
resource "aws_glue_catalog_database" "gold_db" {
  name = "logidata_gold_db"
}

# Crawler para registrar las tablas de la capa Gold
resource "aws_glue_crawler" "gold_crawler" {
  database_name = aws_glue_catalog_database.gold_db.name
  name          = "logidata_gold_crawler"
  role          = var.glue_role_arn

  # Configuración para que entienda el formato Delta
  delta_target {
    delta_tables = ["s3://${var.gold_bucket}/resumen_operativo/"]
    write_manifest = false
    create_native_delta_table = true
  }
  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}