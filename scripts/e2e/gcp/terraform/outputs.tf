output "driver_name" {
  value = google_compute_instance.driver.name
}

output "network_name" {
  value = google_compute_network.e2e.name
}

output "subnet_name" {
  value = google_compute_subnetwork.e2e.name
}

output "bucket_name" {
  value = google_storage_bucket.e2e.name
}

output "driver_orchestrator_service_account_email" {
  value = google_service_account.driver_orchestrator.email
}

output "coordinator_runtime_service_account_email" {
  value = google_service_account.coordinator_runtime.email
}

output "worker_runtime_service_account_email" {
  value = google_service_account.worker_runtime.email
}
