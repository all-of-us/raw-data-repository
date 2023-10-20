import datetime
import json
from cerberus import Validator
from airflow import models
from airflow.operators import python
from google.cloud import bigquery


class EtMValidator(Validator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url_validator = Validator(kwargs.get("metadata_schema"))

    def _check_with_url(self, field, url):
        # print(self.document)
        if "valueString" in self.document:
            answer_value = self.document["valueString"]
        elif "valueDecimal" in self.document:
            answer_value = self.document["valueDecimal"]
        self.url_validator.validate({url: answer_value})
        if self.url_validator.errors:
            self._error(field, str(self.url_validator.errors))

    def _check_with_outcomes_metadata(self, field, value_string):
        self.url_validator.validate(json.loads(value_string))
        if self.url_validator.errors:
            self._error(field, str(self.url_validator.errors))


default_dag_args = {"start_date": datetime.datetime(2023, 8, 1)}

questionnaire_type_map = {
    "emorecog": "emotion_recognition",
    "delaydiscount": "delay_discount",
    "flanker": "flanker",
    "GradCPT": "grad_cpt",
}

with models.DAG(
    "etm_validation",
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_dag_args,
) as dag:

    def validate_payload():
        import logging

        logging.info("Starting validation")
        client = bigquery.Client()
        for questionnaire_type in questionnaire_type_map.keys():
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("qtype", "STRING", questionnaire_type)
                ]
            )
            results_to_validate = client.query(
                """SELECT
                          *
                        FROM
                          rdr_etm_test.etm_questionnaire_response eqr
                        LEFT JOIN
                          rdr_etm_test.participant p
                        ON
                          eqr.participant_id = p.participant_id
                        WHERE
                          eqr.questionnaire_type = @qtype
                          AND eqr.etm_questionnaire_response_id NOT IN (
                          SELECT
                            etm_questionnaire_response_id
                          FROM
                            rdr_etm_test.etm_validation_result)""",
                job_config=job_config,
            )
            to_validate_list = results_to_validate.result()
            eqrs_to_validate = list(to_validate_list)
            query_job = client.query(
                "SELECT * FROM rdr_etm_test.etm_validation_schema "
                "WHERE questionnaire_type=@qtype "
                "ORDER BY valid_from DESC LIMIT 1",
                job_config=job_config,
            )
            result = query_job.result()
            schemas = list(result)
            schema_dict = json.loads(schemas[0].get("fhir_schema"))
            metadata_dict = json.loads(schemas[0].get("metadata_schema"))
            task_schema_version = schemas[0].get("task_schema_version")
            v = EtMValidator(schema_dict, metadata_schema=metadata_dict)
            for eqr in eqrs_to_validate:
                resource_dict = json.loads(eqr.resource)
                print(
                    f"Validating {eqr.get('etm_questionnaire_response_id')}, {eqr.get('questionnaire_type')}"
                )
                v.validate(resource_dict)
                logging.info(f"{eqr.etm_questionnaire_response_id} Errors: {v.errors}")
                errors = str(v.errors) if v.errors else None
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "eqrid", "INT64", eqr.etm_questionnaire_response_id
                        ),
                        bigquery.ScalarQueryParameter(
                            "q_type", "STRING", eqr.questionnaire_type
                        ),
                        bigquery.ScalarQueryParameter(
                            "resource", "JSON", eqr.resource.replace("\n", "")
                        ),
                        bigquery.ScalarQueryParameter("errors", "STRING", errors),
                        bigquery.ScalarQueryParameter(
                            "participant_id", "INT64", eqr.participant_id
                        ),
                        bigquery.ScalarQueryParameter(
                            "task_schema_name", "STRING", task_schema_version
                        ),
                        bigquery.ScalarQueryParameter(
                            "eqr_date", "TIMESTAMP", eqr.created
                        ),
                        bigquery.ScalarQueryParameter(
                            "eqr_authored", "TIMESTAMP", eqr.authored
                        ),
                        bigquery.ScalarQueryParameter(
                            "research_id", "INT64", eqr.research_id
                        ),
                        bigquery.ScalarQueryParameter(
                            "src_id", "STRING", eqr.participant_origin
                        ),
                        bigquery.ScalarQueryParameter("validation_schema_version", "STRING", "1")
                    ]
                )
                validation_insert_query = client.query(
                    """
                INSERT INTO rdr_etm_test.etm_validation_result
                    (id, created, etm_questionnaire_response_id, questionnaire_type, resource,
                    validation_errors, participant_id, task_schema_version, questionnaire_response_timestamp,
                    questionnaire_response_authored, research_id, src_id, validation_schema_version)
                    VALUES (GENERATE_UUID(), CURRENT_TIMESTAMP(), @eqrid, @q_type, @resource, @errors,
                    @participant_id, @task_schema_name, @eqr_date, @eqr_authored, @research_id, @src_id,
                    @validation_schema_version)""",
                    job_config=job_config,
                )
                validation_insert_query.result()

    def deliver_results():
        client = bigquery.Client()

        for qt, task_name in questionnaire_type_map.items():
            select_job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("qt", "STRING", qt)]
            )
            valid_results_query = client.query(
                f"""
            SELECT * FROM rdr_etm_test.etm_validation_result
            WHERE questionnaire_type = @qt
            AND validation_errors IS NULL
            AND id NOT IN (SELECT id FROM rdr_etm_test.{task_name})
            """,
                select_job_config,
            )
            valid_results = valid_results_query.result()
            for res in valid_results:
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("id", "STRING", res.id),
                        bigquery.ScalarQueryParameter(
                            "participant_id", "INT64", res.participant_id
                        ),
                        bigquery.ScalarQueryParameter(
                            "research_id", "INT64", res.research_id
                        ),
                        bigquery.ScalarQueryParameter("src_id", "STRING", res.src_id),
                        bigquery.ScalarQueryParameter("resource", "JSON", res.resource),
                        bigquery.ScalarQueryParameter(
                            "qr_date", "TIMESTAMP", res.questionnaire_response_timestamp
                        ),
                        bigquery.ScalarQueryParameter(
                            "task_schema_name", "STRING", res.task_schema_version
                        ),
                        bigquery.ScalarQueryParameter(
                            "validated_time", "TIMESTAMP", res.created
                        ),
                        bigquery.ScalarQueryParameter(
                            "qr_authored",
                            "TIMESTAMP",
                            res.questionnaire_response_authored,
                        ),
                        bigquery.ScalarQueryParameter(
                            "error", "STRING", res.validation_errors
                        ),
                        bigquery.ScalarQueryParameter(
                            "validation_schema_version",
                            "STRING",
                            res.validation_schema_version,
                        ),
                    ]
                )
                insert_job = client.query(
                    f"""
                INSERT INTO rdr_etm_test.{task_name}
                (validation_id, participant_id, research_id, data_origin_id, source_data,
                questionnaire_response_timestamp,
                drc_received_timestamp, source_schema_version, drc_validated_timestamp, created_timestamp,
                drc_validation_schema_version)
                VALUES (@id, @participant_id, @research_id, @src_id, @resource, @qr_authored, @qr_date,
                @task_schema_name, @validated_time, CURRENT_TIMESTAMP(), @validation_schema_version)
                """,
                    job_config=job_config,
                )
                # wait for insert to finish
                insert_job.result()

            error_results_query = client.query(
                f"""
            SELECT * FROM rdr_etm_test.etm_validation_result
            WHERE questionnaire_type = @qt
            AND validation_errors IS NOT NULL
            AND id NOT IN (SELECT id FROM rdr_etm_test.{task_name}_error)
            """,
                select_job_config,
            )
            error_results = error_results_query.result()
            for res in error_results:
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("id", "STRING", res.id),
                        bigquery.ScalarQueryParameter(
                            "participant_id", "INT64", res.participant_id
                        ),
                        bigquery.ScalarQueryParameter(
                            "research_id", "INT64", res.research_id
                        ),
                        bigquery.ScalarQueryParameter("src_id", "STRING", res.src_id),
                        bigquery.ScalarQueryParameter("resource", "JSON", res.resource),
                        bigquery.ScalarQueryParameter(
                            "qr_date", "TIMESTAMP", res.questionnaire_response_timestamp
                        ),
                        bigquery.ScalarQueryParameter(
                            "task_schema_name", "STRING", res.task_schema_version
                        ),
                        bigquery.ScalarQueryParameter(
                            "validated_time", "TIMESTAMP", res.created
                        ),
                        bigquery.ScalarQueryParameter(
                            "qr_authored",
                            "TIMESTAMP",
                            res.questionnaire_response_authored,
                        ),
                        bigquery.ScalarQueryParameter(
                            "error", "STRING", res.validation_errors
                        ),
                        bigquery.ScalarQueryParameter(
                            "validation_schema_version",
                            "STRING",
                            res.validation_schema_version,
                        ),
                    ]
                )
                insert_job = client.query(
                    f"""
                INSERT INTO rdr_etm_test.{task_name}_error
                (validation_id, participant_id, research_id, data_origin_id, source_data,
                questionnaire_response_timestamp,
                drc_received_timestamp, source_schema_version, drc_validated_timestamp, created_timestamp, error,
                drc_validation_schema_version)
                VALUES (@id, @participant_id, @research_id, @src_id, @resource, @qr_authored, @qr_date,
                @task_schema_name, @validated_time, CURRENT_TIMESTAMP(), @error, @validation_schema_version)
                """,
                    job_config=job_config,
                )
                # wait for insert to finish
                insert_job.result()

    validation_task = python.PythonOperator(
        task_id="validate", python_callable=validate_payload
    )

    delivery_task = python.PythonOperator(
        task_id="deliver", python_callable=deliver_results
    )

    validation_task >> delivery_task
