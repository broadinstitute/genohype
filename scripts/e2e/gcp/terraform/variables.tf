variable "project_id" {
  description = "Disposable E2E target project. Do not use a production project."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project_id))
    error_message = "project_id must be a syntactically valid lowercase GCP project ID."
  }
}

variable "region" {
  type = string

  validation {
    condition     = can(regex("^[a-z]+-[a-z]+[0-9]$", var.region))
    error_message = "region must look like us-east1."
  }
}

variable "zone" {
  type = string

  validation {
    condition     = can(regex("^[a-z]+-[a-z]+[0-9]-[a-z]$", var.zone))
    error_message = "zone must look like us-east1-b; run.sh also verifies it belongs to region."
  }
}

variable "run_id" {
  description = "Unique lowercase identifier used in every resource name."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{5,24}$", var.run_id))
    error_message = "run_id must be 6-25 lowercase letters, digits, or hyphens and start with a letter."
  }
}

variable "driver_machine_type" {
  type    = string
  default = "e2-standard-2"

  validation {
    condition     = contains(["e2-standard-2", "e2-standard-4"], var.driver_machine_type)
    error_message = "driver_machine_type must be an allowlisted E2 standard type."
  }
}
