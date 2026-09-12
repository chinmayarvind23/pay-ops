variable "enabled" {
  description = "Explicit opt-in for a separate operator's billable GCP deployment."
  type        = bool
  default     = false
}

variable "project_id" {
  description = "An existing operator-owned GCP project; this module does not enable billing."
  type        = string
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project_id))
    error_message = "Use an existing valid GCP project ID."
  }
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "operator_cidr" {
  description = "One operator IPv4 /32 allowed to reach the Kubernetes API."
  type        = string
  validation {
    condition     = can(cidrhost(var.operator_cidr, 0)) && endswith(var.operator_cidr, "/32")
    error_message = "Set a single valid IPv4 /32; broad public API access is not supported."
  }
}
