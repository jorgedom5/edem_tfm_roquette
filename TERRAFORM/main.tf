resource "google_bigquery_dataset" "dataset" {
  dataset_id = "datos"
  location   = "europe-southwest1"
}

resource "google_bigquery_table" "bronze_data" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "bronze_data"

  schema = jsonencode([
    {
      "name": "Timestamp",
      "type": "TIMESTAMP",
      "mode": "NULLABLE"
    },
    {
      "name": "Value",
      "type": "FLOAT",
      "mode": "NULLABLE"
    },
    {
      "name": "Tag",
      "type": "STRING",
      "mode": "NULLABLE"
    }
  ])
}

resource "google_bigquery_table" "col_tag" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "col_tag"

  schema = jsonencode([
    {
      "name": "tag",
      "type": "STRING",
      "mode": "NULLABLE"
    },
    {
      "name": "descripcion",
      "type": "STRING",
      "mode": "NULLABLE"
    },
    {
      "name": "sI_NO",
      "type": "STRING",
      "mode": "NULLABLE"
    }
  ])
}

resource "google_bigquery_table" "prediction_results_top" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "prediction_results_top"

  schema = jsonencode([
    {
      "name": "DayHourMinute",
      "type": "STRING",
      "mode": "NULLABLE"
    },
    {
      "name": "Probability_that_flag_is_1",
      "type": "FLOAT",
      "mode": "NULLABLE"
    },
    {
      "name": "Top_1",
      "type": "STRING",
      "mode": "NULLABLE"
    },
    {
      "name": "Top_2",
      "type": "STRING",
      "mode": "NULLABLE"
    },
    {
      "name": "Top_3",
      "type": "STRING",
      "mode": "NULLABLE"
    },
    {
      "name": "Top_4",
      "type": "STRING",
      "mode": "NULLABLE"
    },
    {
      "name": "Top_5",
      "type": "STRING",
      "mode": "NULLABLE"
    }
  ])
}

resource "google_bigquery_table" "recuperaciones_data" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "recuperaciones_data"

  schema = jsonencode([
    {
      "name": "Timestamp",
      "type": "TIMESTAMP",
      "mode": "NULLABLE"
    },
    {
      "name": "Value",
      "type": "FLOAT",
      "mode": "NULLABLE"
    },
    {
      "name": "Tag",
      "type": "STRING",
      "mode": "NULLABLE"
    }
  ])
}

resource "google_bigquery_table" "unit_tag" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "unit_tag"

  schema = jsonencode([
    {
      "name": "Name",
      "type": "STRING",
      "mode": "NULLABLE"
    },
    {
      "name": "ip_eng_units",
      "type": "STRING",
      "mode": "NULLABLE"
    }
  ])
}

resource "google_bigquery_table" "tabla_aux_visualizacion" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "tabla_aux_visualizacion"

  schema = jsonencode([
    {
      "name": "Timestamp",
      "type": "TIMESTAMP",
      "mode": "NULLABLE"
    },
    {
      "name": "Value",
      "type": "INTEGER",
      "mode": "NULLABLE"
    },
    {
      "name": "Predict",
      "type": "INTEGER",
      "mode": "NULLABLE"
    }
  ])
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
