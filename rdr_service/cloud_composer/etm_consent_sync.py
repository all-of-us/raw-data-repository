import datetime
from airflow import models
from google.cloud import bigquery

# NB: Add validation time to validation delivery

# Add full payload to etm consent

default_dag_args = {
    "start_date": datetime.datetime(2023, 8, 1)
}

with models.DAG(
    "etm_consent_sync",
    schedule_interval = datetime.timedelta(days=1),
    default_args=default_dag_args
) as dag:
    def sync_consents():
        client = bigquery.Client()

        latest_consent_query = client.query(
            """SELECT MAX(rdr_created) AS max_created FROM rdr_etm_test.etm_consent;"""
        )
        latest_consent_row = latest_consent_query.result()
        latest_consent = list(latest_consent_row)[0].get("max_created")

        etm_mysql_query = (
            f"""SELECT pa.participant_id AS person_id, pa.research_id, qr.authored, ac.value AS """
            f"""consent_status, pa.participant_origin AS src_id, qr.created AS rdr_created"""
            f""" FROM rdr.participant pa JOIN rdr.questionnaire_response qr ON """
            f"""pa.participant_id = qr.participant_id JOIN rdr.questionnaire_response_answer qra ON """
            f"""qr.questionnaire_response_id = qra.questionnaire_response_id JOIN """
            f"""rdr.questionnaire_question qq ON qra.question_id = qq.questionnaire_question_id """
            f"""JOIN rdr.code qcd ON qq.code_id = qcd.code_id """
            f"""LEFT JOIN rdr.code ac ON qra.value_code_id = ac.code_id JOIN """
            f"""rdr.questionnaire q ON qr.questionnaire_id = q.questionnaire_id JOIN """
            f"""rdr.questionnaire_concept qc ON q.questionnaire_id = qc.questionnaire_id AND """
            f"""q.version = qc.questionnaire_version JOIN rdr.code cc ON qc.code_id = cc.code_id WHERE """
            f"""ac.value IS NOT NULL AND cc.value = 'english_exploring_the_mind_consent_form' """
            f"""AND qcd.value = 'etm_consent' AND qr.created > '{latest_consent}' """
            f""" ORDER BY """
            f"""pa.participant_id, qr.authored;"""
        )
        job_config = bigquery.QueryJobConfig(
            destination="all-of-us-rdr-sandbox.rdr_etm_test.etm_consent",
            write_disposition="WRITE_APPEND",
        )
        query_job = client.query(
            f"""
                SELECT * FROM EXTERNAL_QUERY("all-of-us-rdr-sandbox.us-central1.aou_sandbox_rdrmaindb",
                "{etm_mysql_query}");""",
            job_config=job_config,
        )
        query_job.result()


