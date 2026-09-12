output "cluster_name" {
  value = try(google_container_cluster.payops[0].name, null)
}

output "evidence_bucket" {
  value = try(google_storage_bucket.evidence[0].name, null)
}

output "incident_subscription" {
  value = try(google_pubsub_subscription.worker[0].name, null)
}
