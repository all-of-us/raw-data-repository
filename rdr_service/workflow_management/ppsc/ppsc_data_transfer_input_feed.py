import logging
from abc import ABC, abstractmethod

from google.cloud import bigquery
from werkzeug.exceptions import BadRequest

from rdr_service import config
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.ppsc_partner_transfer_dao import PPSCDataTransferBaseDao
from rdr_service.dao.awardee_insite_dao import AwardeeInSiteDao
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.awardee_insite import AwardeeInSite
from rdr_service.model.ppsc_partner_data_transfer import PPSCCore, PPSCBiobankSample, PPSCHealthData, PPSCEHR
from rdr_service.workflow_management.ppsc import data_feed_queries
from rdr_service.workflow_management.ppsc.ppsc_intake_to_ps_queries import get_consent_activity_to_stream, \
    get_profile_updates_activity_to_stream, get_withdrawal_activity_to_stream, get_deactivation_activity_to_stream, \
    get_participant_status_activity_to_stream, get_survey_completion_activity_to_stream, \
    get_attribution_activity_to_stream, insert_intake_summary_records_sent
from rdr_service.workflow_management.ppsc.ppsc_to_legacy_de_mappings import map_source_to_summary, \
    consent_data_elements, withdrawal_data_elements, profile_updates_data_elements, deactivation_data_elements, \
    participant_status_data_elements, survey_completion_data_elements, attribution_data_elements, \
    map_source_to_participant

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
        return self.bq_client.query(job_def).result()

    def transform_bq_row_to_dict(self, row):
        return {key: row[key] for key in row.keys()}

    def get_datafeed_definition(self, datafeed) -> dict:
        src = config.getSettingJson(config.PPSC_DATAFEED_SRC_DATASET)[0]
        sent_table_name = "intake_summary_datafeed_sent"
        if datafeed == "Consent":
            temp_table_name = "temp_ranked_events_consent"
            source_data_sql = get_consent_activity_to_stream(project=self.project,
                                                                        source_dataset=src,
                                                                        temp_table_name=temp_table_name,
                                                                        sent_table_name=sent_table_name)
            destination_model = ParticipantSummary
            de_mapping = consent_data_elements

        elif datafeed == "Profile Updates":
            temp_table_name = "temp_ranked_events_profile_updates"
            source_data_sql = get_profile_updates_activity_to_stream(project=self.project,
                                                                        source_dataset=src,
                                                                        temp_table_name=temp_table_name,
                                                                        sent_table_name=sent_table_name)
            destination_model = ParticipantSummary
            de_mapping = profile_updates_data_elements

        elif datafeed == "Withdrawal":
            temp_table_name = "temp_ranked_events_withdrawal"
            source_data_sql = get_withdrawal_activity_to_stream(project=self.project,
                                                                        source_dataset=src,
                                                                        temp_table_name=temp_table_name,
                                                                        sent_table_name=sent_table_name)
            destination_model = ParticipantSummary
            de_mapping = withdrawal_data_elements
        elif datafeed == "Deactivation":
            temp_table_name = "temp_ranked_events_deactivation"
            source_data_sql = get_deactivation_activity_to_stream(project=self.project,
                                                                        source_dataset=src,
                                                                        temp_table_name=temp_table_name,
                                                                        sent_table_name=sent_table_name)
            destination_model = ParticipantSummary
            de_mapping = deactivation_data_elements

        elif datafeed == "Participant Status":
            temp_table_name = "temp_ranked_events_participant_status"
            source_data_sql = get_participant_status_activity_to_stream(project=self.project,
                                                                        source_dataset=src,
                                                                        temp_table_name=temp_table_name,
                                                                        sent_table_name=sent_table_name)

            destination_model = ParticipantSummary
            de_mapping = participant_status_data_elements

        elif datafeed == "Survey Completion":
            temp_table_name = "temp_ranked_events_survey_completion"
            source_data_sql = get_survey_completion_activity_to_stream(project=self.project,
                                                                        source_dataset=src,
                                                                        temp_table_name=temp_table_name,
                                                                        sent_table_name=sent_table_name)
            destination_model = ParticipantSummary
            de_mapping = survey_completion_data_elements

        elif datafeed == "Attribution":
            temp_table_name = "temp_ranked_events_attribution"
            source_data_sql = get_attribution_activity_to_stream(project=self.project,
                                                                        source_dataset=src,
                                                                        temp_table_name=temp_table_name,
                                                                        sent_table_name=sent_table_name)
            destination_model = ParticipantSummary
            de_mapping = attribution_data_elements

        else:
            return {}

        insert_sent_sql = insert_intake_summary_records_sent(
            project=self.project,
            source_dataset=src,
            sent_table_name=sent_table_name,
            temp_table_name=temp_table_name,
            datafeed=datafeed
        )

        return {
            "source_data": source_data_sql,
            "temp_select": f"SELECT * FROM `{self.project}.{src}.{temp_table_name}`",
            "temp_table_name": temp_table_name,
            "sent_table_name": sent_table_name,
            "insert_sent_sql": insert_sent_sql,
            "destination_model": destination_model,
            "de_mapping": de_mapping
        }

    def run_datafeed(self, datafeed):
        job_def = self.get_datafeed_definition(datafeed)

        if not job_def:
            logging.warning(f"Could not run {datafeed} because of invalid config")
            return

        # Create the temp table in BigQuery
        logging.info(f"Creating temp table for {datafeed}")
        self.make_datafeed_job(job_def['source_data'])
        logging.info(f"Temp table created for {datafeed}")

        # Get Source Data
        # Retrieve source data from the temp table
        source_data = list(self.make_datafeed_job(job_def['temp_select']))

        if source_data:
            logging.info(f"{datafeed} Source Data retrieved.")
            mapping_args = {}

            if datafeed == "Attribution":
                # Preload organization cache
                dao = OrganizationDao()
                orgs = dao.get_all()
                org_cache = {org.externalId: org.organizationId for org in orgs}
                mapping_args.update({"org_cache": org_cache})

            # Insert into Cloud SQL Table
            summary_dao = ParticipantSummaryDao()
            participant_dao = ParticipantDao()

            with summary_dao.session() as summary_session, participant_dao.session() as participant_session:
                for row in source_data:
                    # Transform source data into the expected dictionary format
                    record = self.transform_bq_row_to_dict(row)

                    # Map ParticipantSummary fields
                    summary_record = map_source_to_summary(
                        record=record,
                        data_element_mapping=job_def['de_mapping'],
                        **mapping_args
                    )
                    summary_session.merge(summary_record)

                    # Map Participant fields (only if test_account is present)
                    try:
                        if record.get("test_account") is not None:
                            participant_record = map_source_to_participant(
                                record=record,
                                data_element_mapping={"test_account": participant_status_data_elements["test_account"]}
                            )
                            participant_session.merge(participant_record)
                    except Exception as e:  # pylint: disable=broad-except
                        logging.error(e)

                # Commit the updates
                summary_session.commit()
                logging.info("ParticipantSummary updates committed.")

                participant_session.commit()
                logging.info("Participant updates committed.")

                logging.info(f"{len(source_data)} {datafeed} records updated.")

            # Insert processed records into the datafeed_sent table
            logging.info(f"Inserting processed records into {job_def['sent_table_name']}")
            self.make_datafeed_job(job_def['insert_sent_sql'])
            logging.info(f"Processed records inserted into {job_def['sent_table_name']}")

        else:
            logging.warning(f"No Staged Rows for {datafeed} Data Feed")


class AwardeeInSiteFeed(PPSCBigQueryDatafeedBase):

    def __init__(self, project='test'):
        self.project = project
        self.bq_client = bigquery.Client()

    def make_datafeed_job(self, job_def: str):
        """Runs the query in BQ and returns the result."""
        return self.bq_client.query(job_def).result()

    def get_datafeed_definition(self) -> dict:
        src = config.getSettingJson(config.PPSC_DATAFEED_SRC_DATASET)[0]
        destination = config.getSettingJson(config.PPSC_DATAFEED_DEST_DATASET)[0]

        job_def = {
            "staging_data_sql": data_feed_queries.insert_awardee_insite_data(
                self.project, src, destination
            ),
            "streaming_data_sql": data_feed_queries.get_awardee_insite_data_to_stream(
                self.project, destination
            ),
            "destination_model": AwardeeInSite,
        }

        return job_def

    @staticmethod
    def row_to_dict(row: bigquery.Row) -> dict:
        row_dict = {}
        for key in row.keys():
            row_dict[key] = row[key]
        return row_dict

    def run_datafeed(self, datafeed: str) -> None:
        # destination = config.getSettingJson(config.PPSC_DATAFEED_DEST_DATASET)[0]

        datafeed_def = self.get_datafeed_definition()
        self.make_datafeed_job(datafeed_def["staging_data_sql"])  # Stage data rows
        # self.make_datafeed_job(
        #     data_feed_queries.update_table_for_withdrawn_participant(self.project, destination)
        # )
        streaming_data_rows = self.make_datafeed_job(datafeed_def["streaming_data_sql"])

        dao = AwardeeInSiteDao()
        if streaming_data_rows:
            for row in streaming_data_rows:
                awardee_insite_dict = AwardeeInSiteFeed.row_to_dict(row)
                id_ = dao.get_id(AwardeeInSite(**awardee_insite_dict))
                if id_:
                    # This allows to update an existing record in MySQL
                    awardee_insite_dict["id"] = id_
                dao.upsert(AwardeeInSite(**awardee_insite_dict))
        else:
            logging.info(f"No rows to add to {datafeed} Data Feed")
