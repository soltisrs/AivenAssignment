 # Creating all Aiven resources 
 
 terraform {
  required_providers {
    aiven = {
      source  = "aiven/aiven"
      version = "~> 3.0"
    }
  }
}

# Token in terraform.tfvars file (omitted for security)
provider "aiven" {
  api_token = var.aiven_api_token
}


variable "project_name" {
  type = string
  default = "clickstream-analytics"
}

# Kafka Service
resource "aiven_kafka" "kafka_service" {
  project = var.project_name
  cloud_name = "google-us-central1"
  plan = "business-4"
  service_name = "kafka-clickstream"

  kafka_user_config {
    kafka {
      auto_create_topics_enable = true
    }
  }
}

# PostgreSQL Service
resource "aiven_pg" "postgres_service" {
  project = var.project_name
  cloud_name = "google-us-central1"
  plan = "startup-4"
  service_name = "postgres-clickstream"
}

# Create Kafka Topic
resource "aiven_kafka_topic" "clickstream_topic" {
  project = var.project_name
  service_name = aiven_kafka.kafka_service.service_name
  topic_name = "clickstream_events"
  partitions = 3
  replication = 2
  
  config {
    retention_ms = 604800000  # 7 days
    cleanup_policy = "delete"
  }
}
# Create Opensearch Service
resource "aiven_opensearch" "opensearch" {
  project      = var.project_name
  service_name = "demo-opensearch"
  plan         = "startup-4"
  cloud_name   = "google-us-central1"
}
