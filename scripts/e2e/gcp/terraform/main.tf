data "google_project" "target" {
  project_id = var.project_id
}

locals {
  prefix = "gh-e2e-${var.run_id}"
  labels = {
    purpose = "genohype-e2e"
    run_id  = var.run_id
  }
  service_account_roles = toset([
    "roles/compute.admin",
    "roles/compute.osAdminLogin",
    "roles/iap.tunnelResourceAccessor",
    "roles/serviceusage.serviceUsageConsumer",
  ])
}

resource "google_service_account" "e2e" {
  account_id   = substr(local.prefix, 0, 30)
  display_name = "Disposable Genohype E2E ${var.run_id}"
}

# Additive members only: never replace an existing project IAM binding/policy.
resource "google_project_iam_member" "e2e" {
  for_each = local.service_account_roles
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.e2e.email}"
}

# Scope actAs to the disposable account itself instead of granting it access to
# every service account in the project.
resource "google_service_account_iam_member" "self_use" {
  service_account_id = google_service_account.e2e.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.e2e.email}"
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

resource "google_storage_bucket_iam_member" "e2e_objects" {
  bucket = google_storage_bucket.e2e.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.e2e.email}"
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

  network_interface {
    subnetwork = google_compute_subnetwork.e2e.id
    # Outbound internet is needed to install system packages. Inbound SSH is
    # still restricted to IAP by the only TCP/22 firewall rule in this VPC.
    access_config {}
  }

  service_account {
    email  = google_service_account.e2e.email
    scopes = ["cloud-platform"]
  }

  metadata = {
    enable-oslogin = "TRUE"
  }

  depends_on = [
    google_project_iam_member.e2e,
    google_service_account_iam_member.self_use,
    google_storage_bucket_iam_member.e2e_objects,
  ]
}
