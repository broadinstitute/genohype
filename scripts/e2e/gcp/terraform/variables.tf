variable "project_id" {
  description = "Disposable E2E target project. Do not use a production project."
  type        = string
}

variable "region" {
  type = string
}

variable "zone" {
  type = string
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
}
