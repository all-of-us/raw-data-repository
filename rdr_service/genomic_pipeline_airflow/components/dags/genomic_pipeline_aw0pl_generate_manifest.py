import tempfile
from datetime import datetime
from google.cloud import storage
from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.operators.python import PythonOperator


PROJECT_ID = Variable.get("gcp_project_id", default_var="my-project")
BUCKET_NAME = Variable.get(
    "biobank_gcs_bucket_name", default_var="my-destination-bucket")

DATASET = "rdr_operational_datastream"
BQ_LOCATION = "us-central1"
GCP_CONN_ID = "google_cloud_default"

EXPORT_TABLE_ID = "rdr_genomic_pipeline_aw0pl_export_tmp"
EXPORT_TABLE = f"{PROJECT_ID}.{DATASET}.{EXPORT_TABLE_ID}"

RAW_URI = (
    f"gs://{BUCKET_NAME}/genomic_samples_manifests/plating/"
    "Genomic-Manifest-AoU-{{ ds }}_C3-{{ ts_nodash | truncate(12, False, '') }}pl.csv"
)

CRLF_URI = (
    f"gs://{BUCKET_NAME}/genomic_samples_manifests/plating/"
    "Genomic-Manifest-AoU-{{ ds }}_C3-{{ ts_nodash | truncate(12, False, '') }}pl.csv"
)


def convert_gcs_lf_to_crlf(input_uri: str, output_uri: str, **context) -> None:
    # pylint: disable=unused-argument
    # Parse gs://bucket/path URIs
    def parse_gs_uri(uri: str):
        assert uri.startswith("gs://"), f"Invalid GCS URI: {uri}"
        without_scheme = uri[len("gs://"):]
        bucket, _, blob = without_scheme.partition("/")
        return bucket, blob

    in_bucket_name, in_blob_name = parse_gs_uri(input_uri)
    out_bucket_name, out_blob_name = parse_gs_uri(output_uri)

    client = storage.Client()
    in_bucket = client.bucket(in_bucket_name)
    in_blob = in_bucket.blob(in_blob_name)

    # Download to temp file
    with tempfile.NamedTemporaryFile("rb+") as tmp_in:
        in_blob.download_to_file(tmp_in)
        tmp_in.flush()
        tmp_in.seek(0)

        # Read and convert line endings
        data = tmp_in.read().decode("utf-8")
        # Normalize to LF then to CRLF
        data = data.replace("\r\n", "\n").replace("\r", "\n")
        crlf_data = data.replace("\n", "\r\n")

        # Upload to output blob
        out_bucket = client.bucket(out_bucket_name)
        out_blob = out_bucket.blob(out_blob_name)
        out_blob.upload_from_string(crlf_data, content_type="text/csv")


BQ_SQL = """
DECLARE batch_id STRING DEFAULT GENERATE_UUID();

CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset }}.{{ params.export_table_id }}` AS
WITH source_rows AS (
  SELECT
    collection_tube_id,
    biobank_id,
    sex_at_birth,
    genome_type,
    ny_flag,
    validation_passed,
    ai_an,
    pediatric,
    finalized,
    created,
    file_path,
    ROW_NUMBER() OVER (
      PARTITION BY genome_type
      ORDER BY created DESC
    ) AS rn
  FROM `{{ params.project_id }}.{{ params.dataset }}.rdr_genomic_pipeline_aw0_tmp`
      WHERE collection_tube_id IN UNNEST([
       {% for id in params.collection_tube_ids %}
         '{{ id }}'{% if not loop.last %},{% endif %}
       {% endfor %}
     ])
),
latest_aw0 AS (
  SELECT
    collection_tube_id,
    biobank_id,
    sex_at_birth,
    genome_type,
    ny_flag,
    validation_passed,
    ai_an,
    pediatric,
    finalized,
    created,
    file_path
  FROM source_rows
  WHERE rn = 1
),
delta AS (
  SELECT
    collection_tube_id,
    biobank_id,
    sex_at_birth,
    genome_type,
    ny_flag,
    validation_passed,
    ai_an,
    pediatric,
    finalized,
    created,
    file_path,
    CURRENT_TIMESTAMP() AS run_time
  FROM latest_aw0 s
  WHERE NOT EXISTS (
    SELECT 1
    FROM `{{ params.project_id }}.{{ params.dataset }}.rdr_genomic_pipeline_aw0_plating` l
    WHERE l.collection_tube_id = s.collection_tube_id
      AND l.genome_type = s.genome_type
      AND l.file_path = s.file_path
  )
)
SELECT
  collection_tube_id,
  biobank_id,
  sex_at_birth,
  genome_type,
  ny_flag,
  validation_passed,
  ai_an,
  pediatric,
  finalized,
  created,
  file_path
FROM delta
;

INSERT INTO `{{ params.project_id }}.{{ params.dataset }}.rdr_genomic_pipeline_aw0_plating` (
  collection_tube_id,
  biobank_id,
  sex_at_birth,
  genome_type,
  ny_flag,
  validation_passed,
  ai_an,
  pediatric,
  finalized,
  created,
  file_path,
  batch_id,
  export_timestamp
)
SELECT
  collection_tube_id,
  biobank_id,
  sex_at_birth,
  genome_type,
  ny_flag,
  validation_passed,
  ai_an,
  pediatric,
  finalized,
  CAST(created AS TIMESTAMP),
  file_path,
  batch_id,
  CURRENT_TIMESTAMP()
FROM `{{ params.project_id }}.{{ params.dataset }}.{{ params.export_table_id }}`
;
"""

with DAG(
    dag_id="genomic_pipeline_aw0pl_generate_manifest",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "collection_tube_ids": Param(
            default=[],
            type="array",
            items={"type": "string"},
            description="List of collection_tube_id values to process"
        )
    },
    tags=["genomics", "aw0", "bigquery", "gcs"],
) as dag:

    run_aw0_query = BigQueryInsertJobOperator(
        task_id="run_aw0_query",
        gcp_conn_id=GCP_CONN_ID,
        location=BQ_LOCATION,
        configuration={
            "query": {
                "query": BQ_SQL,
                "useLegacySql": False,
            }
        },
        params={
            "project_id": PROJECT_ID,
            "dataset": DATASET,
            "export_table_id": EXPORT_TABLE_ID,
        },
    )

    export_aw0_to_gcs = BigQueryToGCSOperator(
        task_id="export_aw0_to_gcs",
        gcp_conn_id=GCP_CONN_ID,
        source_project_dataset_table=EXPORT_TABLE,
        destination_cloud_storage_uris=[RAW_URI],
        export_format="CSV",
        field_delimiter=",",
        print_header=True,
        location=BQ_LOCATION,
    )

    convert_csv_to_crlf = PythonOperator(
        task_id="convert_csv_to_crlf",
        python_callable=convert_gcs_lf_to_crlf,
        op_kwargs={
            "input_uri": RAW_URI,
            "output_uri": CRLF_URI,
        },
    )

    run_aw0_query >> export_aw0_to_gcs >> convert_csv_to_crlf
