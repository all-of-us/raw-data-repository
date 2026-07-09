from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os
import logging

DATASET = "rdr_operational_datastream"


BQ_INITIAL_VALIDATION_SQL = """
update  `{{ params.project_id }}.{{ params.dataset }}.rdr_genomic_pipeline_aw0_tmp` as target
set validation_passed = 'N'
from (
with ranked_withdrawal_events as
                        (select distinct aw0t.biobank_id,sswe.event_type_name,
                                RANK() OVER (PARTITION BY sswe.participant_id, sswe.event_type_name, sswe.data_element_name
                                ORDER BY sswe.event_authored_time DESC, sswe.event_id DESC ) as withdrawal_rank, sswe.data_element_value
                            from  `{{ params.project_id }}.{{ params.dataset }}.rdr_genomic_pipeline_aw0_tmp` aw0t
                                  JOIN  `{{ params.project_id }}.{{ params.dataset }}.ppsc_participant` p ON aw0t.biobank_id = concat('A',SAFE_CAST(p.biobank_id as STRING))
                                   join `{{ params.project_id }}.{{ params.dataset }}.rdr_genomic_pipeline_collection_tube_ids_aw0_plating` plat on
aw0t.biobank_id = concat('A',SAFE_CAST(p.biobank_id as STRING))
                                  Join `{{ params.project_id }}.{{ params.dataset }}.ppsc_withdrawal_event` sswe on p.id = sswe.participant_id
                             And sswe.data_element_name = 'activity_status' and sswe.event_type_name = 'Withdrawal' and sswe.ignore_flag = 0
                             where validation_passed = 'Y'
                               )
select biobank_id from ranked_withdrawal_events where withdrawal_rank = 1) as source
WHERE target.biobank_id = source.biobank_id;"""


with DAG(
    dag_id="genomic_pipeline_aw0pl_update_validation_passed.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["genomics", "aw0"],
) as dag:
    logger = logging.getLogger("airflow.task")
    logger.info(f"Updating validation passed flag for all genomic members")
    env = os.environ.get('GENOMIC_PIPELINE_ENV', 'GENOMIC_PIPELINE_ENV')
    gcp_genomic_environment = os.environ.get('GCP_PROJECT')

    select_task = BigQueryInsertJobOperator(
        task_id='update_validation_passed',
        configuration={
            "query": {
                "query": BQ_INITIAL_VALIDATION_SQL,
                "useLegacySql": False,
                "priority": "BATCH"
            }
        },
        location='us-central1',
        deferrable=False,
        params={
            "dataset": DATASET,
            "project_id":  gcp_genomic_environment
        },
    )
    trigger_child = TriggerDagRunOperator(
        task_id="trigger_aw0pl_manifest",
        trigger_dag_id="genomic_pipeline_aw0pl_generate_manifest",  # Must match the child's dag_id
        wait_for_completion=False,  # If True, parent waits for child to finish
    )

