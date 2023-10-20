import datetime
from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCreateEmptyTableOperator
)

INSERT_QUERY = """
SELECT  rp.participant_id,
        rp.research_id,
        CURRENT_TIMESTAMP() AS created_timestamp,
        CAST(rp.last_modified AS TIMESTAMP) AS rdr_last_modified_timestamp,
        CAST(rp.sign_up_time AS TIMESTAMP) AS sign_up_timestamp,
        CASE WHEN rp.participant_origin = 'careevolution' THEN 'ce'
             ELSE rp.participant_origin
        END AS data_origin_id,
        CASE WHEN IFNULL(rp.is_ghost_id, 0) = 1 THEN 1
             WHEN IFNULL(rp.is_test_participant, 0) = 1 THEN 1
             ELSE 0
        END AS test_participant,
        CAST(rp.withdrawal_status AS STRING) AS withdrawal_status,
        CAST(rp.withdrawal_authored AS TIMESTAMP) AS withdrawal_authored_timestamp,
        CAST(rp.withdrawal_reason AS STRING) AS withdrawal_reason,
        rp.withdrawal_reason_justification,
        CAST(rp.suspension_status AS STRING) AS deactivation_status,
        CAST(rp.suspension_time AS TIMESTAMP) AS deactivation_authored_timestamp,
        rp.version
FROM datastream_testing.rdr_participant rp
LEFT JOIN rdr_etm_test.participant_ops po ON rp.participant_id = po.participant_id AND rp.version = po.version
WHERE po.participant_id IS NULL;
"""

PARTICIPANT_OPS_SCHEMA = [
    {"name": "participant_id", "type": "INT64", "mode": "NULLABLE"},
    {"name": "research_id", "type": "INT64", "mode": "NULLABLE"},
    {"name": "created_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "rdr_last_modified_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "sign_up_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "data_origin_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "test_participant", "type": "INT64", "mode": "NULLABLE"},
    {"name": "withdrawal_status", "type": "STRING", "mode": "NULLABLE"},
    {"name": "withdrawal_authored_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "withdrawal_reason", "type": "STRING", "mode": "NULLABLE"},
    {"name": "withdrawal_reason_justification", "type": "STRING", "mode": "NULLABLE"},
    {"name": "deactivation_status", "type": "STRING", "mode": "NULLABLE"},
    {
        "name": "deactivation_authored_timestamp",
        "type": "TIMESTAMP",
        "mode": "NULLABLE",
    },
    {"name": "version", "type": "INT64", "mode": "NULLABLE"},
]
default_dag_args = {"start_date": datetime.datetime(2023, 10, 1)}

with models.DAG(
    "participant_ops",
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_dag_args,
) as dag:
    insert_query_job = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration={
            "query": {
                "query": INSERT_QUERY,
                "useLegacySql": False,
                "writeDisposition": "WRITE_APPEND",
                "destinationTable": {
                    "projectId": "all-of-us-rdr-sandbox",
                    "datasetId": "rdr_etm_test",
                    "tableId": "participant_ops",
                },
            }
        },
    )

    create_table_job = BigQueryCreateEmptyTableOperator(
        task_id="create_table_job",
        dataset_id="rdr_etm_test",
        table_id="participant_ops",
        project_id="all-of-us-rdr-sandbox",
        schema_fields=PARTICIPANT_OPS_SCHEMA,
        if_exists="ignore"
    )

    [create_table_job >> insert_query_job]
