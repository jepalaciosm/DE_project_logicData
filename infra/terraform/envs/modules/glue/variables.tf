variable "project" {}
variable "env" {}
variable "glue_role_arn" {}
variable "script_location" {}
variable "script_catalogo_etl_broze_silver" {}
variable "script_etl_silver_gold" {}

variable "bronze_bucket" {}
variable "silver_bucket" {}
variable "gold_bucket" {}
variable "quarantine_bucket" {}
variable "temp_bucket" {}

variable "tags" {
  type = map(string)
}