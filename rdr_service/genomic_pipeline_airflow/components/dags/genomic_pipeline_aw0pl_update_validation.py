import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

MERGE_SQL = """
    MERGE `{{ params.project_id }}.{{ params.dataset }}.rdr_genomic_pipeline_gsm_validation_history` T
    USING (
      SELECT distinct
       aw0tmp.biobank_id,
       ai.withdrawal_status,
       ai.withdrawal_time
     FROM `{{ params.project_id }}.{{ params.dataset }}.rdr_genomic_pipeline_aw0_tmp` aw0tmp
        JOIN `{{ params.project_id }}.{{ params.dataset }}.rdr_biobank_stored_sample` ss
            on concat('A', SAFE_CAST(ss.biobank_id as STRING)) = aw0tmp.biobank_id
        JOIN  `{{ params.project_id }}.{{ params.dataset }}.ppsc_participant` p ON ss.biobank_id = p.biobank_id
        JOIN `{{ params.project_id }}.{{ params.dataset }}.ppsc_awardee_insite` ai on p.id = ai.participant_id
        where ai.withdrawal_time > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 7 DAY)
    ) S
    ON T.biobank_id = S.biobank_id
    WHEN MATCHED THEN UPDATE SET
      T.biobank_id = S.biobank_id,
      T.withdrawal_status= S.withdrawal_status,
      T.withdrawal_time = S.withdrawal_time
      WHEN NOT MATCHED THEN INSERT (
      biobank_id,  withdrawal_status,
      withdrawal_time)
     VALUES (S.biobank_id, S.withdrawal_status, S.withdrawal_time);
;
"""

with DAG(
    dag_id="genomic_pipeline_aw0_update_validation_history",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["genomics", "aw0"],
) as dag:
    env = os.environ.get('GENOMIC_PIPELINE_ENV', 'GENOMIC_PIPELINE_ENV')
    gcp_genomic_environment = os.environ.get('GCP_PROJECT')
    logger = logging.getLogger("airflow.task")
    logger.info(f"environement is {env}")

    run_update_validation = BigQueryInsertJobOperator(
        task_id="run_aw0_query",
        configuration={
            "query": {
                "query": MERGE_SQL,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location='us-central1',
        deferrable=False)
