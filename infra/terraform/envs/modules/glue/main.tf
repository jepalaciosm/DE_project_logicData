resource "aws_glue_job" "catalogo_etl" {
  name     = "${var.project}-${var.env}-catalogo-etl"
  role_arn = var.glue_role_arn

  command {
    script_location = var.script_location
    python_version  = "3"
  }


  default_arguments = {
    "--input_path"  = "s3://${var.bronze_bucket}/catalogo.csv"
    "--output_path" = "s3://${var.silver_bucket}/catalogo/"
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
    "--input_path"  = "s3://${var.bronze_bucket}/catalogo.csv"
    "--output_path" = "s3://${var.silver_bucket}/catalogo/"
    "--quarantine_path" = "s3://${var.quarantine_bucket}/catalogo/"
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