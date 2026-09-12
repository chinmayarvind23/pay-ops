# Mock-provider plans validate expressions and safeguards without cloud API calls.
mock_provider "google" {}

variables {
  project_id    = "payops-test-project"
  operator_cidr = "192.0.2.1/32"
}

run "disabled_by_default" {
  command = plan
  assert {
    condition     = length(google_container_cluster.payops) == 0 && length(google_storage_bucket.evidence) == 0 && length(google_pubsub_topic.incidents) == 0 && length(google_project_service.required) == 0
    error_message = "Default configuration must not create billable resources or enable APIs."
  }
}

run "explicit_foundation" {
  command = plan
  variables {
    enabled = true
  }
  assert {
    condition     = google_container_cluster.payops[0].deletion_protection && google_container_cluster.payops[0].enable_autopilot
    error_message = "The reviewed cluster shape and deletion protection must remain explicit."
  }
  assert {
    condition     = google_storage_bucket.evidence[0].public_access_prevention == "enforced" && google_storage_bucket.evidence[0].uniform_bucket_level_access && !google_storage_bucket.evidence[0].force_destroy
    error_message = "Evidence must not become public or silently delete on teardown."
  }
  assert {
    condition     = one(google_container_cluster.payops[0].master_authorized_networks_config[0].cidr_blocks).cidr_block == "192.0.2.1/32"
    error_message = "Kubernetes API access must stay bound to the operator address."
  }
}
