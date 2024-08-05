resource "google_bigquery_dataset" "dataset" {
  dataset_id = "datos"
  location   = "europe-southwest1"
}

resource "google_storage_bucket" "bronze_zone_roquette" {
  name     = "bronze_zone_roquette"
  location = "europe-southwest1"
}

resource "google_storage_bucket" "model_roquette" {
  name     = "model_roquette"
  location = "europe-southwest1"
}

resource "google_storage_bucket" "gcf_v2_sources" {
  name     = "gcf-v2-sources-14707834508-europe-southwest1"
  location = "europe-southwest1"
}


resource "google_cloudfunctions_function" "csv_to_bq" {
  name        = "csv_to_bq"
  description = "Function to move CSV from Storage to BigQuery"
  runtime     = "python311"
  region      = "europe-southwest1"
  source_archive_bucket = "gcf-v2-sources-14707834508-europe-southwest1"
  source_archive_object = "csv_to_bq/function-source.zip"
  entry_point            = "process_csv_files"

  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = "projects/_/buckets/bronze_zone_roquette"
  }
}




resource "google_artifact_registry_repository" "api" {
  provider   = google-beta
  location   = "europe-southwest1"
  repository_id = "api"
  format     = "DOCKER"
}

resource "google_artifact_registry_repository" "repo_model" {
  provider   = google-beta
  location   = "europe-southwest1"
  repository_id = "repo-model"
  format     = "DOCKER"
}
