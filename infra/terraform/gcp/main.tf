terraform {
  required_version = ">= 1.10, < 2.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.29"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# The module creates nothing unless a separate operator explicitly enables it.
# No credential files, application secrets or public workload services are managed here.
resource "google_project_service" "required" {
  for_each = var.enabled ? toset([
    "compute.googleapis.com", "container.googleapis.com", "storage.googleapis.com",
    "pubsub.googleapis.com", "iam.googleapis.com"
  ]) : toset([])
  service            = each.value
  disable_on_destroy = false
}

resource "google_compute_network" "payops" {
  count                   = var.enabled ? 1 : 0
  name                    = "payops"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.required]
}

resource "google_compute_subnetwork" "payops" {
  count                    = var.enabled ? 1 : 0
  name                     = "payops"
  ip_cidr_range            = "10.40.0.0/20"
  region                   = var.region
  network                  = google_compute_network.payops[0].id
  private_ip_google_access = true
  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.44.0.0/16"
  }
  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.48.0.0/20"
  }
}

resource "google_service_account" "nodes" {
  count        = var.enabled ? 1 : 0
  account_id   = "payops-gke-nodes"
  display_name = "PayOps GKE nodes"
  depends_on   = [google_project_service.required]
}

resource "google_project_iam_member" "node_metrics" {
  for_each = var.enabled ? toset([
    "roles/logging.logWriter", "roles/monitoring.metricWriter",
    "roles/container.defaultNodeServiceAccount"
  ]) : toset([])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.nodes[0].email}"
}

resource "google_container_cluster" "payops" {
  count               = var.enabled ? 1 : 0
  name                = "payops"
  location            = var.region
  enable_autopilot    = true
  deletion_protection = true
  network             = google_compute_network.payops[0].id
  subnetwork          = google_compute_subnetwork.payops[0].id
  release_channel {
    channel = "REGULAR"
  }
  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }
  cluster_autoscaling {
    auto_provisioning_defaults {
      service_account = google_service_account.nodes[0].email
      oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    }
  }
  master_authorized_networks_config {
    cidr_blocks {
      cidr_block   = var.operator_cidr
      display_name = "operator"
    }
  }
  depends_on = [google_project_iam_member.node_metrics]
}

# Bucket IAM is deliberately absent: a deployer must bind a scoped workload identity.
resource "google_storage_bucket" "evidence" {
  count                       = var.enabled ? 1 : 0
  name                        = "${var.project_id}-payops-evidence"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  versioning {
    enabled = true
  }
  depends_on = [google_project_service.required]
}

resource "google_pubsub_topic" "incidents" {
  count      = var.enabled ? 1 : 0
  name       = "payops-incidents"
  depends_on = [google_project_service.required]
}

resource "google_pubsub_subscription" "worker" {
  count                      = var.enabled ? 1 : 0
  name                       = "payops-worker"
  topic                      = google_pubsub_topic.incidents[0].id
  ack_deadline_seconds       = 60
  message_retention_duration = "86400s"
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}
