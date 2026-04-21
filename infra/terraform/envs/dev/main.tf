data "aws_caller_identity" "current" {}
 

module "bronze_bucket" {
  source      = "../modules/s3_lake"
  project     = var.project
  env         = var.env
  bucket_name = "bronze"
  account_id  = data.aws_caller_identity.current.account_id
  tags        = var.tags
}

module "silver_bucket" {
  source      = "../modules/s3_lake"
  project     = var.project
  env         = var.env
  bucket_name = "silver"
  account_id  = data.aws_caller_identity.current.account_id
  tags        = var.tags
}

module "gold_bucket" {
  source      = "../modules/s3_lake"
  project     = var.project
  env         = var.env
  bucket_name = "gold"
  account_id  = data.aws_caller_identity.current.account_id
  tags        = var.tags
}

module "quarantine_bucket" {
  source      = "../modules/s3_lake"
  project     = var.project
  env         = var.env
  bucket_name = "quarantine"
  account_id  = data.aws_caller_identity.current.account_id
  tags        = var.tags
}
module "glue_job" {
  source = "../modules/glue"

  project = var.project
  env     = var.env
  glue_role_arn = module.iam.glue_role_arn

  bronze_bucket = module.bronze_bucket.bucket_name
  silver_bucket = module.silver_bucket.bucket_name
  gold_bucket = module.gold_bucket.bucket_name
  temp_bucket   = module.bronze_bucket.bucket_name
  quarantine_bucket = module.quarantine_bucket.bucket_name

  script_location = "s3://${module.bronze_bucket.bucket_name}/scripts/etl_catalogo.py"
  script_catalogo_etl_broze_silver = "s3://${module.bronze_bucket.bucket_name}/scripts/catalogo_bronze_to_silver.py"
  script_etl_silver_gold = "s3://${module.silver_bucket.bucket_name}/scripts/silver_to_gold.py" 
  tags = var.tags
}

module "glue_job2" {
  source = "../modules/glue"

  project = var.project
  env     = var.env
  glue_role_arn = module.iam.glue_role_arn

  bronze_bucket = module.bronze_bucket.bucket_name
  silver_bucket = module.silver_bucket.bucket_name
  gold_bucket = module.gold_bucket.bucket_name
  temp_bucket   = module.bronze_bucket.bucket_name
  quarantine_bucket = module.quarantine_bucket.bucket_name

  script_location = "s3://${module.bronze_bucket.bucket_name}/scripts/catalogo_bronze_to_silver.py"
  script_catalogo_etl_broze_silver = "s3://${module.bronze_bucket.bucket_name}/scripts/catalogo_bronze_to_silver.py"
  script_etl_silver_gold = "s3://${module.silver_bucket.bucket_name}/scripts/silver_to_gold.py"
  tags = var.tags
}

module "glue_job3" {
  source = "../modules/glue"

  project = var.project
  env     = var.env
  glue_role_arn = module.iam.glue_role_arn
  bronze_bucket = module.bronze_bucket.bucket_name
  silver_bucket = module.silver_bucket.bucket_name
  gold_bucket = module.gold_bucket.bucket_name
  temp_bucket   = module.bronze_bucket.bucket_name
  quarantine_bucket = module.quarantine_bucket.bucket_name

  script_location = "s3://${module.silver_bucket.bucket_name}/scripts/silver_to_gold.py"
  script_etl_silver_gold = "s3://${module.silver_bucket.bucket_name}/scripts/silver_to_gold.py"
  script_catalogo_etl_broze_silver = "s3://${module.bronze_bucket.bucket_name}/scripts/catalogo_bronze_to_silver.py"

  tags = var.tags
}


module "iam" {
  source = "../modules/iam"

  project = var.project
  env     = var.env

  bronze_bucket = module.bronze_bucket.bucket_name
  silver_bucket = module.silver_bucket.bucket_name
  gold_bucket = module.gold_bucket.bucket_name
  quarantine_bucket = module.quarantine_bucket.bucket_name
  temp_bucket   = module.bronze_bucket.bucket_name
}