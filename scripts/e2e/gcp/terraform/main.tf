data "google_project" "target" {
  project_id = var.project_id
}

locals {
  prefix = "gh-e2e-${var.run_id}"
  labels = {
    purpose = "genohype-e2e"
    run_id  = var.run_id
  }
  driver_project_roles = toset([
    "roles/compute.instanceAdmin.v1",
    "roles/compute.osAdminLogin",
    "roles/iap.tunnelResourceAccessor",
    "roles/serviceusage.serviceUsageConsumer",
  ])
}

resource "google_service_account" "driver_orchestrator" {
  account_id   = "ghd-${var.run_id}"
  display_name = "Genohype E2E driver ${var.run_id}"
}

resource "google_service_account" "coordinator_runtime" {
  account_id   = "ghc-${var.run_id}"
  display_name = "Genohype E2E coordinator ${var.run_id}"
}

resource "google_service_account" "worker_runtime" {
  account_id   = "ghw-${var.run_id}"
  display_name = "Genohype E2E worker ${var.run_id}"
}

# Additive members only: never replace an existing project IAM binding/policy.
resource "google_project_iam_member" "driver" {
  for_each = local.driver_project_roles
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.driver_orchestrator.email}"
}

resource "google_project_iam_member" "coordinator_viewer" {
  project = var.project_id
  role    = "roles/compute.viewer"
  member  = "serviceAccount:${google_service_account.coordinator_runtime.email}"
}

# The driver can attach only the two disposable runtime identities, never an
# arbitrary account in the target project.
resource "google_service_account_iam_member" "driver_uses_coordinator" {
  service_account_id = google_service_account.coordinator_runtime.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.driver_orchestrator.email}"
}

resource "google_service_account_iam_member" "driver_uses_worker" {
  service_account_id = google_service_account.worker_runtime.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.driver_orchestrator.email}"
}

resource "google_compute_network" "e2e" {
  name                    = "${local.prefix}-net"
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"
}

resource "google_compute_subnetwork" "e2e" {
  name                     = "${local.prefix}-subnet"
  region                   = var.region
  network                  = google_compute_network.e2e.id
  ip_cidr_range            = "10.252.0.0/24"
  private_ip_google_access = true
}

resource "google_compute_subnetwork_iam_member" "driver_network_user" {
  project    = var.project_id
  region     = var.region
  subnetwork = google_compute_subnetwork.e2e.name
  role       = "roles/compute.networkUser"
  member     = "serviceAccount:${google_service_account.driver_orchestrator.email}"
}

resource "google_compute_router" "e2e" {
  name    = "${local.prefix}-router"
  region  = var.region
  network = google_compute_network.e2e.id
}

resource "google_compute_router_nat" "e2e" {
  name                               = "${local.prefix}-nat"
  router                             = google_compute_router.e2e.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "LIST_OF_SUBNETWORKS"

  subnetwork {
    name                    = google_compute_subnetwork.e2e.id
    source_ip_ranges_to_nat = ["ALL_IP_RANGES"]
  }
}

resource "google_compute_firewall" "iap_ssh" {
  name      = "${local.prefix}-iap-ssh"
  network   = google_compute_network.e2e.name
  direction = "INGRESS"

  source_ranges = ["35.235.240.0/20"]
  target_tags = [
    "genohype-e2e-driver",
    "genohype-coordinator",
    "genohype-worker",
  ]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

resource "google_compute_firewall" "coordinator_internal" {
  name      = "${local.prefix}-coordinator"
  network   = google_compute_network.e2e.name
  direction = "INGRESS"

  source_ranges = [google_compute_subnetwork.e2e.ip_cidr_range]
  target_tags   = ["genohype-coordinator"]

  allow {
    protocol = "tcp"
    ports    = ["3000"]
  }
}

resource "google_storage_bucket" "e2e" {
  name                        = "genohype-e2e-${data.google_project.target.number}-${var.run_id}"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
  labels                      = local.labels

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket_iam_member" "driver_objects" {
  bucket = google_storage_bucket.e2e.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.driver_orchestrator.email}"
}

resource "google_storage_bucket_iam_member" "coordinator_objects" {
  bucket = google_storage_bucket.e2e.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.coordinator_runtime.email}"
}

resource "google_storage_bucket_iam_member" "worker_read" {
  bucket = google_storage_bucket.e2e.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.worker_runtime.email}"
}

resource "google_storage_bucket_iam_member" "worker_create" {
  bucket = google_storage_bucket.e2e.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.worker_runtime.email}"
}

resource "google_compute_instance" "driver" {
  name         = "${local.prefix}-driver"
  zone         = var.zone
  machine_type = var.driver_machine_type
  tags         = ["genohype-e2e-driver"]
  labels       = local.labels

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2404-lts-amd64"
      size  = 30
      type  = "pd-balanced"
    }
  }

  # The driver is private. Cloud NAT supplies egress and IAP supplies inbound SSH.
  network_interface {
    subnetwork = google_compute_subnetwork.e2e.id
  }

  service_account {
    email  = google_service_account.driver_orchestrator.email
    scopes = ["cloud-platform"]
  }

  metadata = {
    enable-oslogin = "TRUE"
  }

  depends_on = [
    google_compute_router_nat.e2e,
    google_project_iam_member.driver,
    google_compute_subnetwork_iam_member.driver_network_user,
    google_service_account_iam_member.driver_uses_coordinator,
    google_service_account_iam_member.driver_uses_worker,
    google_storage_bucket_iam_member.driver_objects,
  ]
}
