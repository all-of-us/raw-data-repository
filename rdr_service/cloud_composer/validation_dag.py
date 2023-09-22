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

default_dag_args = {
    "start_date": datetime.datetime(2023, 8, 1)
}

with models.DAG(
    "etm_validation",
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_dag_args
) as dag:
    def validate_payload():
        import logging
        logging.info("Starting validation")
        client = bigquery.Client()

        results_to_validate = client.query(
            "SELECT * FROM rdr_etm_test.etm_questionnaire_response WHERE "
            "questionnaire_type = 'emorecog'"
            "AND etm_questionnaire_response_id NOT IN (SELECT "
            "etm_questionnaire_response_id FROM rdr_etm_test.etm_validation_result) "
        )
        to_validate_list = results_to_validate.result()
        eqrs_to_validate = list(to_validate_list)
        query_job = client.query("SELECT * FROM rdr_etm_test.etm_validation_schema WHERE questionnaire_type='emorecog'")
        result = query_job.result()
        schemas = list(result)
        schema_dict = json.loads(schemas[0].get('fhir_schema'))
        metadata_dict = json.loads(schemas[0].get('metadata_schema'))
        task_schema_version = schemas[0].get('task_schema_version')
        v = EtMValidator(schema_dict, metadata_schema=metadata_dict)
        for eqr in eqrs_to_validate:
            resource_dict = json.loads(eqr.resource)
            print(f"Validating {eqr.get('etm_questionnaire_response_id')}, {eqr.get('questionnaire_type')}")
            v.validate(resource_dict)
            validation_time = datetime.datetime.utcnow().isoformat()
            logging.info(f"{eqr.etm_questionnaire_response_id} Errors: {v.errors}")
            errors = str(v.errors) if v.errors else None
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("eqrid", "INT64", eqr.etm_questionnaire_response_id),
                    bigquery.ScalarQueryParameter("q_type", "STRING", eqr.questionnaire_type),
                    bigquery.ScalarQueryParameter("resource", "JSON", eqr.resource.replace('\n','')),
                    bigquery.ScalarQueryParameter("validation_time", "TIMESTAMP", validation_time),
                    bigquery.ScalarQueryParameter("errors", "STRING", errors),
                    bigquery.ScalarQueryParameter("participant_id", "INT64", eqr.participant_id),
                    bigquery.ScalarQueryParameter("task_schema_name", "STRING", task_schema_version),
                    bigquery.ScalarQueryParameter("eqr_date", "TIMESTAMP", eqr.created)
                ]
            )
            validation_insert_query = client.query("""
            INSERT INTO rdr_etm_test.etm_validation_result
                (id, created, etm_questionnaire_response_id, questionnaire_type, resource, validation_time,
                validation_errors, participant_id, task_schema_version, questionnaire_response_date)
                VALUES (GENERATE_UUID(), CURRENT_TIMESTAMP(), @eqrid, @q_type, @resource, @validation_time, @errors,
                @participant_id, @task_schema_name, @eqr_date)""",
                                                   job_config=job_config)
            validation_insert_query.result()

    def deliver_results():
        client = bigquery.Client()
        questionnaire_type_map = {
            'emorecog': 'emotion_recognition'
        }

        for qt, task_name in questionnaire_type_map.items():
            select_job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("qt", "STRING", qt)
                ]
            )
            valid_results_query = client.query(f"""
            SELECT * FROM rdr_etm_test.etm_validation_result
            WHERE questionnaire_type = @qt
            AND validation_errors IS NULL
            AND id NOT IN (SELECT id FROM rdr_etm_test.{task_name})
            """, select_job_config)
            valid_results = valid_results_query.result()
            research_id = 0
            src_id = 'test'
            for res in valid_results:
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("id", "STRING", res.id),
                        bigquery.ScalarQueryParameter("participant_id", "INT64", res.participant_id),
                        bigquery.ScalarQueryParameter("research_id", "INT64", research_id),
                        bigquery.ScalarQueryParameter("src_id", "STRING", src_id),
                        bigquery.ScalarQueryParameter("resource", "JSON", res.resource),
                        bigquery.ScalarQueryParameter("qr_date", "TIMESTAMP",res.questionnaire_response_date),
                        bigquery.ScalarQueryParameter("task_schema_name", "STRING", res.task_schema_version),
                        bigquery.ScalarQueryParameter("validated_time", "TIMESTAMP", res.validation_time)
                    ]
                )
                insert_job = client.query(f"""
                INSERT INTO rdr_etm_test.{task_name}
                (id, participant_id, research_id, src_id, resource, questionnaire_response_date, task_schema_version,
                validated_time, created)
                VALUES (@id, @participant_id, @research_id, @src_id, @resource, @qr_date, @task_schema_name,
                @validated_time, CURRENT_TIMESTAMP())
                """, job_config=job_config)
                # wait for insert to finish
                insert_job.result()


            error_results_query = client.query(f"""
            SELECT * FROM rdr_etm_test.etm_validation_result
            WHERE questionnaire_type = @qt
            AND validation_errors IS NOT NULL
            AND id NOT IN (SELECT id FROM rdr_etm_test.{task_name}_error)
            """, select_job_config)
            error_results = error_results_query.result()
            research_id = 0
            src_id = 'test'
            for res in error_results:
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("id", "STRING", res.id),
                        bigquery.ScalarQueryParameter("participant_id", "INT64", res.participant_id),
                        bigquery.ScalarQueryParameter("research_id", "INT64", research_id),
                        bigquery.ScalarQueryParameter("src_id", "STRING", src_id),
                        bigquery.ScalarQueryParameter("resource", "JSON", res.resource),
                        bigquery.ScalarQueryParameter("qr_date", "TIMESTAMP",res.questionnaire_response_date),
                        bigquery.ScalarQueryParameter("task_schema_name", "STRING", res.task_schema_version),
                        bigquery.ScalarQueryParameter("error", "STRING", res.validation_errors),
                        bigquery.ScalarQueryParameter("validation_time", "TIMESTAMP", res.validation_time)

                    ]
                )
                insert_job = client.query(f"""
                INSERT INTO rdr_etm_test.{task_name}_error
                (id, participant_id, research_id, src_id, resource, questionnaire_response_date, error,
                task_schema_version, validated_time, created)
                VALUES (@id, @participant_id, @research_id, @src_id, @resource, @qr_date, @error, @task_schema_name,
                @validation_time, CURRENT_TIMESTAMP())
                """, job_config=job_config)
                # wait for insert to finish
                insert_job.result()

    validation_task = python.PythonOperator(
        task_id="validate", python_callable=validate_payload
    )

    delivery_task = python.PythonOperator(
        task_id="deliver", python_callable=deliver_results
    )

    validation_task >> delivery_task
