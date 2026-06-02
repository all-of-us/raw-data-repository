from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import os
import logging

with DAG(
    'genomic_pipeline_aw0_initial_selection',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    logger = logging.getLogger("airflow.task")

    env = os.environ.get('GENOMIC_PIPELINE_ENV', 'GENOMIC_PIPELINE_ENV')
    gcp_genomic_environment = os.environ.get('GCP_PROJECT')
    logger.info(f"environement is {env}")
    logger.info(f"gcpenvironement is {gcp_genomic_environment}")

    INSERT_ROWS_QUERY = (f"""
        insert into `{gcp_genomic_environment}.rdr_operational_datastream.rdr_genomic_set_member_pipeline_tmp`
        (created, modified, biobank_id, participant_id, ny_flag, valid_withdrawal_status , general_consent_given,  ai_an,sex_at_birth, genome_type, pediatric,
        genomic_workflow_state,  genomic_workflow_state_str )
        with ranked_consent_events as
                        (select p.id,ssce.event_type_name,
                                RANK() OVER (PARTITION BY ssce.participant_id, ssce.event_type_name, ssce.data_element_name
                                ORDER BY ssce.event_authored_time DESC, ssce.event_id DESC ) as consent_rank, ssce.data_element_value
                            from `{gcp_genomic_environment}.rdr_operational_datastream.rdr_biobank_stored_sample` ss
                                  JOIN  `{gcp_genomic_environment}.rdr_operational_datastream.ppsc_participant` p ON ss.biobank_id = p.biobank_id
                                  Join `{gcp_genomic_environment}.rdr_operational_datastream.ppsc_consent_event` ssce on p.id = ssce.participant_id
                             And ssce.data_element_name = 'activity_status' and ssce.ignore_flag = 0 and ssce.event_type_name in ('Primary Consent','Pediatric Permission')
                                  LEFT JOIN `{gcp_genomic_environment}.rdr_operational_datastream.rdr_genomic_set_member` m ON m.participant_id = p.id
                             AND m.genomic_workflow_state <> 33
                         where
                             ss.test in ('2ED02', '1ED02', '2ED04', '1ED04', '1ED10', '1SAL2', '1SAL', '2SAL0', '3SAL1')
                           AND m.id IS NULL ),
       ranked_withdrawal_events as
                        (select p.id,sswe.event_type_name,
                                RANK() OVER (PARTITION BY sswe.participant_id, sswe.event_type_name, sswe.data_element_name
                                ORDER BY sswe.event_authored_time DESC, sswe.event_id DESC ) as consent_rank, sswe.data_element_value
                            from `{gcp_genomic_environment}.rdr_operational_datastream.rdr_biobank_stored_sample` ss
                                  JOIN  `{gcp_genomic_environment}.rdr_operational_datastream.ppsc_participant` p ON ss.biobank_id = p.biobank_id
                                  Join `{gcp_genomic_environment}.rdr_operational_datastream.ppsc_withdrawal_event` sswe on p.id = sswe.participant_id
                             And sswe.data_element_name = 'activity_status' and sswe.event_type_name = 'Withdrawal' and sswe.ignore_flag = 0
                                  LEFT JOIN `{gcp_genomic_environment}.rdr_operational_datastream.rdr_genomic_set_member` m ON m.participant_id = p.id
                             AND m.genomic_workflow_state <> 33
                         where
                             ss.test in ('2ED02', '1ED02', '2ED04', '1ED04', '1ED10', '1SAL2', '1SAL', '2SAL0', '3SAL1')
                           AND m.id IS NULL ),

        ranked_sex_events as
                        (select p.id,
                                RANK() OVER (PARTITION BY ssce.participant_id, ssce.event_type_name, ssce.data_element_name
                                ORDER BY ssce.event_authored_time DESC, ssce.event_id DESC ) as sex_rank, ssce.data_element_value
                            from `{gcp_genomic_environment}.rdr_operational_datastream.rdr_biobank_stored_sample` ss
                                  JOIN  `{gcp_genomic_environment}.rdr_operational_datastream.ppsc_participant` p ON ss.biobank_id = p.biobank_id
                                  Join `{gcp_genomic_environment}.rdr_operational_datastream.ppsc_survey_completion_event` ssce on p.id = ssce.participant_id
                             And ssce.data_element_name in ('biologicalsexatbirth_sexatbirth','biologicalsexatbirth_sexatbirth_ped') and ssce.ignore_flag = 0
                                  LEFT JOIN `{gcp_genomic_environment}.rdr_operational_datastream.rdr_genomic_set_member` m ON m.participant_id = p.id
                             AND m.genomic_workflow_state <> 33
                         where
                             ss.test in ('2ED02', '1ED02', '2ED04', '1ED04', '1ED10', '1SAL2', '1SAL', '2SAL0', '3SAL1')
                           AND m.id IS NULL ),

                    ranked_aian_events as
                        (select p.id,
                                RANK() OVER (PARTITION BY aice.participant_id ORDER BY aice.event_authored_time DESC, aice.event_id DESC, aice.id desc  ) as aian_rank, aice.data_element_value

                         from `{gcp_genomic_environment}.rdr_operational_datastream.rdr_biobank_stored_sample` ss
                                  JOIN `{gcp_genomic_environment}.rdr_operational_datastream.ppsc_participant` p ON ss.biobank_id = p.biobank_id
                                  Join `{gcp_genomic_environment}.rdr_operational_datastream.ppsc_survey_completion_event`  aice on p.id = aice.participant_id
                          and event_type_name IN ('Basics Data', 'Basics Data Peds 0to6')
                                            AND aice.data_element_name IN ('race_whatraceethnicity', 'race_whatraceethnicity_ped')
                                            AND aice.ignore_flag = 0

                                  LEFT JOIN `{gcp_genomic_environment}.rdr_operational_datastream.rdr_genomic_set_member` m ON m.participant_id = p.id
                             AND m.genomic_workflow_state <> 33
                         where
                             ss.test in ('2ED02', '1ED02', '2ED04', '1ED04', '1ED10', '1SAL2', '1SAL', '2SAL0', '3SAL1')
                           AND m.id IS NULL ),
                           ranked_pediatric_events as
                        (select p.id,sspu.event_type_name,
                                RANK() OVER (PARTITION BY sspu.participant_id, sspu.event_type_name, sspu.data_element_name
                                ORDER BY sspu.event_authored_time DESC, sspu.event_id DESC ) as consent_rank, sspu.data_element_value
                            from `{gcp_genomic_environment}.rdr_operational_datastream.rdr_biobank_stored_sample` ss
                                  JOIN  `{gcp_genomic_environment}.rdr_operational_datastream.ppsc_participant` p ON ss.biobank_id = p.biobank_id
                                  Join `{gcp_genomic_environment}.rdr_operational_datastream.ppsc_profile_updates_event` sspu on p.id = sspu.participant_id
                             And sspu.data_element_name = 'activity_status' and sspu.event_type_name = 'Account Type' and sspu.ignore_flag = 0
                                  LEFT JOIN `{gcp_genomic_environment}.rdr_operational_datastream.rdr_genomic_set_member` m ON m.participant_id = p.id
                             AND m.genomic_workflow_state <> 33
                         where
                             ss.test in ('2ED02', '1ED02', '2ED04', '1ED04', '1ED10', '1SAL2', '1SAL', '2SAL0', '3SAL1')
                           AND m.id IS NULL )

               SELECT DISTINCT CURRENT_DATETIME("America/Chicago") as created,
                CURRENT_DATETIME("America/Chicago") as modified,
               CAST(ss.biobank_id as STRING)  as biobank_id,
                               p.id as participant_id,

                               case when (rs.state = 'NY'or rc.short_value  like '%NY%' ) then 1 else 0 end as ny_flag,


                               CASE
                                   WHEN rwe.data_element_value= 'withdrawn' THEN 0
                                   ELSE 1
                                   END as valid_withdrawal_status,

                                CASE
                                   WHEN lower(rce.data_element_value) like '%yes%' THEN 1
                                   ELSE 0
                                   END as general_consent_given,
                                     CASE
                                   WHEN ra.data_element_value = 'WhatRaceEthnicity_AIAN' then 'Y'
                                   else 'N'
                                   END AS ai_an,
                               CASE
                                   WHEN rse.data_element_value = 'SexAtBirth_Male' THEN 'M'
                                   WHEN rse.data_element_value = 'SexAtBirth_Female' THEN 'F'
                                   ELSE 'NA'
                                   END as sex_at_birth,


                            'aou_array' as genome_type,
                            case when lower(rpe.data_element_value) = 'pediatric' then 'Y' else 'N' end as pediatric,
                            32 as genomic_workflow_state,
                            'AW0_READY' as genomic_workflow_state_str
      FROM `{gcp_genomic_environment}.rdr_operational_datastream.rdr_biobank_stored_sample` ss
                        JOIN `{gcp_genomic_environment}.rdr_operational_datastream.ppsc_participant` p ON ss.biobank_id = p.biobank_id
                        JOIN `{gcp_genomic_environment}.rdr_operational_datastream.rdr_biobank_order_identifier` oi ON ss.biobank_order_identifier = oi.value
                        JOIN `{gcp_genomic_environment}.rdr_operational_datastream.rdr_biobank_order` o ON oi.biobank_order_id = o.biobank_order_id


                        LEFT JOIN `{gcp_genomic_environment}.rdr_operational_datastream.rdr_genomic_set_member` m ON m.participant_id = p.id
                   AND m.genomic_workflow_state <> 33
                        LEFT JOIN `{gcp_genomic_environment}.rdr_operational_datastream.rdr_biobank_mail_kit_order` mk ON mk.participant_id = p.id
                          LEFT JOIN `{gcp_genomic_environment}.rdr_operational_datastream.rdr_site` rs on rs.site_id = o.collected_site_id
                        left join `{gcp_genomic_environment}.rdr_operational_datastream.rdr_code` rc on  rc.code_id = mk.state_id
                        LEFT JOIN `{gcp_genomic_environment}.rdr_operational_datastream.ppsc_participant_status_event` pse on pse.participant_id = p.id

                   AND pse.event_type_name = 'Test Account'
                   AND pse.data_element_name = 'activity_status'
                   AND pse.data_element_value = "test"

                        left join ranked_aian_events ra on ra.id = p.id and ra.aian_rank = 1
                        left join ranked_sex_events rse on rse.id = p.id and rse.sex_rank = 1
                        left join ranked_consent_events rce on rce.id = p.id and rce.consent_rank = 1
                        left join ranked_withdrawal_events rwe on rwe.id = p.id and rwe.consent_rank = 1
                        left join ranked_pediatric_events rpe on rpe.id = p.id and rpe.consent_rank = 1
               WHERE TRUE
                 AND ss.test in ('2ED02', '1ED02', '2ED04', '1ED04', '1ED10', '1SAL2', '1SAL', '2SAL0', '3SAL1')
                 AND m.id IS NULL
                 AND pse.id IS NULL and m.created > DATE_SUB(CURRENT_DATE(), INTERVAL 2230 DAY);

  insert into `{gcp_genomic_environment}.rdr_operational_datastream.rdr_genomic_set_member_pipeline_tmp` (created, modified, biobank_id, participant_id, ny_flag, valid_withdrawal_status , general_consent_given,  ai_an,sex_at_birth, genome_type, pediatric,
 genomic_workflow_state,  genomic_workflow_state_str)
(select created, modified, biobank_id, participant_id, ny_flag, valid_withdrawal_status , general_consent_given,  ai_an,sex_at_birth, 'aou_wgs' as genome_type, pediatric,
 genomic_workflow_state,  genomic_workflow_state_str from `{gcp_genomic_environment}.rdr_operational_datastream.rdr_genomic_set_member_pipeline_tmp`  );""")

    select_task = BigQueryInsertJobOperator(
        task_id='insert_new_participants',
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location='us-central1',
        deferrable=False)
    trigger_child = TriggerDagRunOperator(
        task_id="trigger_child_dag",
        trigger_dag_id="genomic_pipeline_aw0_update_validation",  # Must match the child's dag_id
        conf={"message": "Hello from step1"},  # Optional configuration payload
        wait_for_completion=False,  # If True, parent waits for child to finish
    )
