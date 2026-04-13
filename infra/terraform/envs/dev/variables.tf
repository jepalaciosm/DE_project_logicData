variable "project" {
  type        = string
  description = "Nombre del proyecto"
}

variable "env" {
  type        = string
  description = "Entorno (dev, staging, prod)"
}

variable "tags" {
  type        = map(string)
  description = "Mapa de tags aplicados a recursos"
}
