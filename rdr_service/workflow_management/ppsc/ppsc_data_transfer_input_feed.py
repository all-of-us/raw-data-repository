import logging
from abc import ABC, abstractmethod

from google.cloud import bigquery
from werkzeug.exceptions import BadRequest

from rdr_service import config
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.ppsc_partner_transfer_dao import PPSCDataTransferBaseDao
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.ppsc_partner_data_transfer import PPSCCore, PPSCBiobankSample, PPSCHealthData, PPSCEHR
# from rdr_service.cloud_utils import bigquery
from rdr_service.workflow_management.ppsc import data_feed_queries
from rdr_service.workflow_management.ppsc.ppsc_intake_to_ps_queries import get_consent_activity_to_stream, \
    get_profile_updates_activity_to_stream, get_withdrawal_activity_to_stream
from rdr_service.workflow_management.ppsc.ppsc_to_legacy_de_mappings import map_source_to_summary, \
    consent_data_elements, withdrawal_data_elements, profile_updates_data_elements

datafeeds = [
    "core data",
    "biospecimen",
    "healthdata sharing",
    "ehr"
]


class PPSCBigQueryDatafeedBase(ABC):
    @abstractmethod
    def make_datafeed_job(self, job_def: str):
        ...

    @abstractmethod
    def get_datafeed_definition(self, datafeed: str):
        ...

    @abstractmethod
    def run_datafeed(self, datafeed: str):
        ...


class InputFeed(PPSCBigQueryDatafeedBase):
    def __init__(self, project='test'):
        self.project = project
        self.bq_client = bigquery.Client()

    def make_datafeed_job(self, job_def):
        return self.bq_client.query(job_def)

    def get_datafeed_definition(self, datafeed):
        src = config.getSettingJson(config.PPSC_DATAFEED_SRC_DATASET)[0]
        destination = config.getSettingJson(config.PPSC_DATAFEED_DEST_DATASET)[0]

        job_def = {}

        if datafeed == "core data":
            job_def['staging_data'] = data_feed_queries.insert_core_data(self.project, src, destination)
            job_def['streaming_data'] = data_feed_queries.get_ppsc_core_to_stream(self.project, destination)
            job_def['output_model'] = PPSCCore
            return job_def

        elif datafeed == "biospecimen":
            job_def['staging_data'] = data_feed_queries.insert_biospecimen(self.project, src, destination)
            job_def['streaming_data'] = data_feed_queries.get_ppsc_biospecimen_to_stream(self.project, destination)
            job_def['output_model'] = PPSCBiobankSample
            return job_def

        elif datafeed == "health data sharing":
            job_def['staging_data'] = data_feed_queries.insert_health_data_sharing(self.project, src, destination)
            job_def['streaming_data'] = data_feed_queries.get_health_data_to_stream(self.project, destination)
            job_def['output_model'] = PPSCHealthData
            return job_def

        elif datafeed == "ehr":
            job_def['staging_data'] = data_feed_queries.insert_ehr_receipt(self.project, src, destination)
            job_def['streaming_data'] = data_feed_queries.get_ppsc_ehr_to_stream(self.project, destination)
            job_def['output_model'] = PPSCEHR
            return job_def

        else:
            # Raise error
            raise BadRequest(f"Invalid Datafeed: {datafeed}")

    def run_datafeed(self, datafeed):
        """
        Loads datafeed results in batches and commits updates to database per batch.
        """
        job_def = self.get_datafeed_definition(datafeed)

        # Stage the Data
        job = self.make_datafeed_job(job_def['staging_data'])

        if job is not None:
            logging.info(f"{datafeed} Data Feed Staged")
        else:
            logging.warning(f"Could not run {datafeed} Data Feed because of invalid config")

        streaming_data_rows = list(self.make_datafeed_job(job_def['streaming_data']))

        if streaming_data_rows:
            logging.info(f"{datafeed} Data Feed Staged")
            # Insert into Cloud SQL Table
            rows = [dict(row) for row in streaming_data_rows]
            dao = PPSCDataTransferBaseDao(job_def['output_model'])
            with dao.session() as session:
                session.bulk_insert_mappings(job_def['output_model'], rows)
        else:
            logging.warning(f"No Staged Rows for {datafeed} Data Feed")


class Intake2SummaryFeed(PPSCBigQueryDatafeedBase):
    def __init__(self, project='test'):
        self.project = project
        self.bq_client = bigquery.Client()

    def make_datafeed_job(self, job_def):
        return self.bq_client.query(job_def)

    def get_datafeed_definition(self, datafeed) -> dict:
        src = config.getSettingJson(config.PPSC_DATAFEED_SRC_DATASET)[0]
        if datafeed == "Consent":
            source_data_sql = get_consent_activity_to_stream(project=self.project, source_dataset=src)
            destination_model = ParticipantSummary
            de_mapping = consent_data_elements

        elif datafeed == "Profile Updates":
            source_data_sql = get_profile_updates_activity_to_stream(project=self.project, source_dataset=src)
            destination_model = ParticipantSummary
            de_mapping = profile_updates_data_elements

        elif datafeed == "Withdrawal":
            source_data_sql = get_withdrawal_activity_to_stream(project=self.project, source_dataset=src)
            destination_model = ParticipantSummary
            de_mapping = withdrawal_data_elements

        else:
            return {}

        return {
            "source_data": source_data_sql,
            "destination_model": destination_model,
            "de_mapping": de_mapping
        }

    def run_datafeed(self, datafeed):
        job_def = self.get_datafeed_definition(datafeed)

        if not job_def:
            logging.warning(f"Could not run {datafeed} of invalid config")
            return

        # Get Source Data
        source_data = list(self.make_datafeed_job(job_def['source_data']))

        if source_data:
            logging.info(f"{datafeed} Source Data retrieved.")
            # Insert into Cloud SQL Table
            rows = [dict(row) for row in source_data]

            dao = ParticipantSummaryDao()
            with dao.session() as session:
                for record in rows:
                    summary_record = map_source_to_summary(record, job_def['de_mapping'])

                    session.merge(summary_record)
                # Commit the updates
                session.commit()
                logging.info(f"{len(source_data)} {datafeed} ParticipantSummary records updated.")

        else:
            logging.warning(f"No Staged Rows for {datafeed} Data Feed")


