from datetime import datetime
import json

from google.cloud import bigquery
from cerberus import Validator
import datetime

from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = "airflow-dev"
tool_desc = "Test airflow DAG functions"

emorecog_answer_rules = {
    "https://research.joinallofus.org/fhir/emorecog/image": {"type": "string"},
    "https://research.joinallofus.org/fhir/emorecog/response": {"type": "string"},
    "https://research.joinallofus.org/fhir/emorecog/correct": {"type": "integer"},
    "https://research.joinallofus.org/fhir/emorecog/rt": {"type": "float"},
    "https://research.joinallofus.org/fhir/emorecog/state": {"type": "string"},
    "https://research.joinallofus.org/fhir/emorecog/repeated": {"type": "integer"},
    "https://research.joinallofus.org/fhir/emorecog/flagged": {"type": "integer"},
    "https://research.joinallofus.org/fhir/emorecog/timestamp": {"type": "integer"},
    "https://research.joinallofus.org/fhir/emorecog/trial_id": {"type": "string"},
    "https://research.joinallofus.org/fhir/emorecog/emotion": {"type": "string"},
    "operating_system": {"type": "string"},
    "response_device": {"type": "string"},
    "screen_height": {"type": "float"},
    "screen_width": {"type": "float"},
    "test_duration": {"type": "float"},
    "test_end_date_time": {"type": "float"},
    "test_language": {"type": "string"},
    "test_name": {"type": "string"},
    "test_params": {"type": "string"},
    "test_restarted": {"type": "float"},
    "test_short_name": {"type": "string"},
    "test_start_date_time": {"type": "float"},
    "test_version": {"type": "string"},
    "touch": {"type": "float"},
    "user_agent": {"type": "string"},
    "user_utc_offset": {"type": "float"},
    "accuracy": {"type": "float"},
    "angry_accuracy": {"type": "float"},
    "angry_meanRTc": {"type": "float", "nullable": True},
    "angry_medianRTc": {"type": "float", "nullable": True},
    "angry_sdRTc": {"type": "float", "nullable": True},
    "any_timeouts": {"type": "float"},
    "fearful_accuracy": {"type": "float"},
    "fearful_meanRTc": {"type": "float", "nullable": True},
    "fearful_medianRTc": {"type": "float", "nullable": True},
    "fearful_sdRTc": {"type": "float", "nullable": True},
    "flag_medianRTc": {"type": "float"},
    "flag_sameResponse": {"type": "integer"},
    "flag_trialFlags": {"type": "integer"},
    "happy_accuracy": {"type": "float"},
    "happy_meanRTc": {"type": "float", "nullable": True},
    "happy_medianRTc": {"type": "float", "nullable": True},
    "happy_sdRTc": {"type": "float", "nullable": True},
    "meanRTc": {"type": "float"},
    "medianRTc": {"type": "float"},
    "sad_accuracy": {"type": "float"},
    "sad_meanRTc": {"type": "float", "nullable": True},
    "sad_medianRTc": {"type": "float", "nullable": True},
    "sad_sdRTc": {"type": "float", "nullable": True},
    "score": {"type": "integer"},
    "sdRTc": {"type": "float"},
}

emorecog_validation_schema = {
    "group": {
        "type": "dict",
        "required": True,
        "schema": {
            "linkId": {"type": "string"},
            "question": {
                "type": "list",
                "schema": {
                    "type": "dict",
                    "schema": {
                        "answer": {
                            "type": "list",
                            "schema": {
                                "type": "dict",
                                "schema": {
                                    "valueCoding": {"type": "dict"},
                                    "extension": {
                                        "type": "list",
                                        "schema": {
                                            "type": "dict",
                                            "schema": {
                                                "url": {
                                                    "type": "string",
                                                    "required": True,
                                                    "check_with": "url",
                                                    "allowed": [
                                                        "https://research.joinallofus.org/fhir/emorecog/trial_id",
                                                        "https://research.joinallofus.org/fhir/emorecog/image",
                                                        "https://research.joinallofus.org/fhir/emorecog/emotion",
                                                        "https://research.joinallofus.org/fhir/emorecog/response",
                                                        "https://research.joinallofus.org/fhir/emorecog/correct",
                                                        "https://research.joinallofus.org/fhir/emorecog/rt",
                                                        "https://research.joinallofus.org/fhir/emorecog/state",
                                                        "https://research.joinallofus.org/fhir/emorecog/repeated",
                                                        "https://research.joinallofus.org/fhir/emorecog/flagged",
                                                        "https://research.joinallofus.org/fhir/emorecog/timestamp",
                                                    ],
                                                },
                                                "valueString": {"type": "string"},
                                                "valueDecimal": {"type": "float"},
                                            },
                                        },
                                    },
                                },
                            },
                        },
                        "linkId": {"type": "string"},
                    },
                },
            },
        },
    },
    "status": {"type": "string"},
    "subject": {"type": "dict"},
    "authored": {"type": "string"},
    "extension": {
        "type": "list",
        "schema": {
            "type": "dict",
            "schema": {
                "url": {"type": "string"},
                "valueString": {"type": "string", "check_with": "outcomes_metadata"},
            },
        },
    },
    "identifier": {"type": "dict"},
    "questionnaire": {"type": "dict"},
    "resourceType": {"type": "string"},
}


class EtMValidator(Validator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url_validator = Validator(kwargs.get("metadata_schema"))

    def _check_with_url(self, field, url):
        # print(self.document)
        # print(f"url: {url}")
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


class AirflowTestTool(ToolBase):
    def run(self):
        super(AirflowTestTool, self).run()

        self.validate_payload()
        # self.setup_schema()
        # self.sync_consents()
        # self.deliver_results()

    def setup_schema(self):
        client = bigquery.Client()
        fhir_schema = json.dumps(emorecog_validation_schema)
        metadata_schema = json.dumps(emorecog_answer_rules)

        job_options = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("fhir_schema", "STRING", fhir_schema),
                bigquery.ScalarQueryParameter(
                    "metadata_schema", "STRING", metadata_schema
                ),
                bigquery.ScalarQueryParameter(
                    "task_version", "STRING", "EmoRecog_AoU.v1.May23"
                ),
                bigquery.ScalarQueryParameter(
                    "questionnaire_type", "STRING", "emorecog"
                ),
                bigquery.ScalarQueryParameter(
                    "valid_from", "TIMESTAMP", datetime.datetime(2023, 9, 1)
                ),
            ]
        )
        insert_schema = client.query(
            """
            INSERT INTO rdr_etm_test.etm_validation_schema
            (questionnaire_type, valid_from, created, fhir_schema, metadata_schema, task_schema_version)
            VALUES (@questionnaire_type, @valid_from, CURRENT_TIMESTAMP(), @fhir_schema, @metadata_schema,
            @task_version)
        """,
            job_options,
        )
        insert_schema.result()

    def validate_payload(self):
        import logging

        logging.info("Starting validation")
        dryrun = True
        qtype = "delaydiscount"
        client = bigquery.Client()

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("qtype", "STRING", qtype)
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
            f"SELECT * FROM rdr_etm_test.etm_validation_schema WHERE questionnaire_type='{qtype}'"
        )
        result = query_job.result()
        schemas = list(result)
        # print(schemas[0].get('schema'))
        fhir_schema_dict = json.loads(schemas[0].get("fhir_schema"))
        metadata_schema_dict = json.loads(schemas[0].get("metadata_schema"))
        # print(metadata_schema_dict)
        v = EtMValidator(schema=fhir_schema_dict, metadata_schema=metadata_schema_dict)
        # v.setup_metadata_validator(metadata_schema_dict)
        print("Loaded schema dict")
        for eqr in eqrs_to_validate:
            print(
                f"Validating {eqr.get('etm_questionnaire_response_id')}, {eqr.get('questionnaire_type')}"
            )
            # print(repr(eqr.resource))
            resource_dict = json.loads(eqr.resource)
            v.validate(resource_dict)
            validation_time = datetime.datetime.utcnow().isoformat()
            print(str(v.errors))
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
                    bigquery.ScalarQueryParameter(
                        "validation_time", "TIMESTAMP", validation_time
                    ),
                    bigquery.ScalarQueryParameter("errors", "STRING", str(v.errors)),
                ]
            )
            if not dryrun:
                validation_insert_query = client.query(
                    """
                INSERT INTO rdr_etm_test.etm_validation_result
                    (id, created, etm_questionnaire_response_id, questionnaire_type, resource, validation_time, """
                    """validation_errors) VALUES (GENERATE_UUID(), CURRENT_TIMESTAMP(), @eqrid, @q_type, @resource, """
                    """@validation_time, @errors)""",
                    job_config=job_config,
                )
                validation_insert_query.result()

    def deliver_results(self):
        dryrun = True
        client = bigquery.Client()
        questionnaire_type_map = {"emorecog": "emotion_recognition"}

        for qt, task_name in questionnaire_type_map.items():
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("qt", "STRING", qt)]
            )
            valid_results_query = client.query(
                f"""
            SELECT * FROM rdr_etm_test.etm_validation_result
            WHERE questionnaire_type = @qt
            AND validation_errors IS NULL
            AND id NOT IN (SELECT id FROM rdr_etm_test.{task_name})
            """,
                job_config,
            )
            valid_results = valid_results_query.result()
            for res in valid_results:
                pid_rid_src_job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "pid", "INT64", res.participant_id
                        )
                    ]
                )
                pid_rid_src_query = client.query(
                    """SELECT research_id, participant_origin FROM
                        rdr_etm_test.participant WHERE participant_id = @pid""",
                    job_config=pid_rid_src_job_config,
                )
                prs_result = list(pid_rid_src_query.result())
                print(prs_result)
                if prs_result:
                    research_id = prs_result[0].research_id
                    src_id = prs_result[0].participant_origin
                else:
                    research_id = 0
                    src_id = "test"
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("id", "STRING", res.id),
                        bigquery.ScalarQueryParameter(
                            "participant_id", "INT64", res.participant_id
                        ),
                        bigquery.ScalarQueryParameter(
                            "research_id", "INT64", research_id
                        ),
                        bigquery.ScalarQueryParameter("src_id", "STRING", src_id),
                        bigquery.ScalarQueryParameter("resource", "JSON", res.resource),
                    ]
                )
                if not dryrun:
                    insert_job = client.query(
                        f"""
                    INSERT INTO rdr_etm_test.{task_name}
                    (id, participant_id, research_id, src_id, resource)
                    VALUES (@id, @participant_id, @research_id, @src_id, @resource)
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
                job_config,
            )
            error_results = error_results_query.result()
            for res in error_results:
                pid_rid_src_job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "pid", "INT64", res.participant_id
                        )
                    ]
                )
                pid_rid_src_query = client.query(
                    """SELECT research_id, participant_origin FROM
                            rdr_etm_test.participant WHERE participant_id = @pid""",
                    job_config=pid_rid_src_job_config,
                )
                prs_result = list(pid_rid_src_query.result())
                if prs_result:
                    research_id = prs_result[0].research_id
                    src_id = prs_result[0].participant_origin
                else:
                    research_id = 0
                    src_id = "test"
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("id", "STRING", res.id),
                        bigquery.ScalarQueryParameter(
                            "participant_id", "INT64", res.participant_id
                        ),
                        bigquery.ScalarQueryParameter(
                            "research_id", "INT64", research_id
                        ),
                        bigquery.ScalarQueryParameter("src_id", "STRING", src_id),
                        bigquery.ScalarQueryParameter("resource", "JSON", res.resource),
                    ]
                )
                if not dryrun:
                    insert_job = client.query(
                        f"""
                    INSERT INTO rdr_etm_test.{task_name}_error
                    (id, participant_id, research_id, src_id, resource)
                    VALUES (@id, @participant_id, @research_id, @src_id, @resource)
                    """,
                        job_config=job_config,
                    )
                    # wait for insert to finish
                    insert_job.result()

    def sync_consents(self):
        client = bigquery.Client()
        # latest_consent_query = client.query("""SELECT MAX(rdr_created) AS """\
        # """max_created FROM rdr_etm_test.etm_consent;""")
        # latest_consent_row = latest_consent_query.result()
        # latest_consent = list(latest_consent_row)[0].get("max_created")
        # latest_consent = '2020-01-01'

        etm_mysql_query = (
            f"""SELECT pa.participant_id AS person_id, pa.research_id, qr.authored, ac.value AS """
            """consent_status, pa.participant_origin AS src_id, qr.created AS rdr_created"""
            """ FROM rdr.participant pa JOIN rdr.questionnaire_response qr ON """
            """pa.participant_id = qr.participant_id JOIN rdr.questionnaire_response_answer qra ON """
            """qr.questionnaire_response_id = qra.questionnaire_response_id JOIN """
            """rdr.questionnaire_question qq ON qra.question_id = qq.questionnaire_question_id """
            """JOIN rdr.code qcd ON qq.code_id = qcd.code_id """
            """LEFT JOIN rdr.code ac ON qra.value_code_id = ac.code_id JOIN """
            """rdr.questionnaire q ON qr.questionnaire_id = q.questionnaire_id JOIN """
            """rdr.questionnaire_concept qc ON q.questionnaire_id = qc.questionnaire_id AND """
            """q.version = qc.questionnaire_version JOIN rdr.code cc ON qc.code_id = cc.code_id WHERE """
            """ac.value IS NOT NULL AND cc.value = 'english_exploring_the_mind_consent_form' """
            """AND qcd.value = 'etm_consent' AND qr.created > '{latest_consent}' """
            """ ORDER BY """
            """pa.participant_id, qr.authored;"""
        )
        job_config = bigquery.QueryJobConfig(
            destination="all-of-us-rdr-sandbox.rdr_etm_test.etm_consent",
            write_disposition="WRITE_APPEND",
        )
        query_job = client.query(
            f"""
        SELECT * FROM EXTERNAL_QUERY("all-of-us-rdr-prod.us-central1.bq-rdr-preprod-curation",
        "{etm_mysql_query}");""",
            job_config=job_config,
        )
        query_job.result()


def run():
    return cli_run(tool_cmd, tool_desc, AirflowTestTool)
