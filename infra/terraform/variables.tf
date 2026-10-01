variable "project" {
  type    = string
  default = "retail-sales"
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "environments" {
  description = "One S3 prefix (and one Redshift database) per environment."
  type        = list(string)
  default     = ["dev", "prod"]
}

variable "redshift_azs" {
  description = "AZs that support Redshift Serverless in this region (need >= 3)."
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

variable "allowed_cidr" {
  description = "Your public IP as a /32 -- the only address allowed to reach Redshift. `curl -s https://checkip.amazonaws.com`"
  type        = string
}

variable "redshift_admin_username" {
  type    = string
  default = "admin"
}

variable "redshift_admin_password" {
  description = "8-64 chars, at least one upper, one lower, one digit."
  type        = string
  sensitive   = true
}

variable "redshift_base_rpu" {
  description = "Smallest allowed base capacity keeps per-second cost down."
  type        = number
  default     = 8
}

variable "redshift_daily_rpu_hours" {
  description = "Hard daily compute cap; the workgroup deactivates when exceeded."
  type        = number
  default     = 3
}
