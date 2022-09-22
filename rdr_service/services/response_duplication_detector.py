from datetime import datetime, timedelta
from time import sleep
import logging
from sqlalchemy import and_, func, update
from sqlalchemy.orm import aliased
from typing import Type

from rdr_service.config import GAE_PROJECT
from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask
from rdr_service.dao.database_factory import get_database
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseClassificationType
from rdr_service.model.bigquery_sync import BigQuerySync
from rdr_service.model.bq_base import BQTable
from rdr_service.resource.tasks import batch_rebuild_participants_task
from rdr_service.services.system_utils import list_chunks


class ResponseDuplicationDetector:
    def __init__(self, duplication_threshold: int = 2):
        """
        Used to check the database for any new questionnaire response duplicates.

        :param duplication_threshold: The number of matching responses needed in a group before any of them will be
            considered duplicates. Any responses that have already been marked as duplicates count toward this total.
            Defaults to 2.
        """
        self.duplication_threshold = duplication_threshold

    @classmethod
    def _responses_are_duplicates(cls, newer_response: Type[QuestionnaireResponse],
                                  older_response: Type[QuestionnaireResponse]):
        return and_(
            newer_response.created > older_response.created,
            newer_response.externalId == older_response.externalId,
            newer_response.answerHash == older_response.answerHash,
            newer_response.participantId == older_response.participantId
        )

    def _get_duplicate_responses(self, session, earliest_response_date):
        older_duplicate = aliased(QuestionnaireResponse)  # joined as older responses to be updated as duplicates
        newer_duplicate = aliased(QuestionnaireResponse)  # used to keep COMPLETE classificiaton on the latest response
        other_duplicate = aliased(QuestionnaireResponse)  # used to find the number of other duplicates there are
        return (
            session.query(
                QuestionnaireResponse.questionnaireResponseId,  # The latest one
                func.group_concat(older_duplicate.questionnaireResponseId.distinct()),  # Responses to mark as dups
                func.count(other_duplicate.questionnaireResponseId.distinct())  # Total number of duplicates
            ).with_hint(
                QuestionnaireResponse,
                'USE INDEX (idx_created_q_id)'
            ).join(
                older_duplicate,
                and_(
                    self._responses_are_duplicates(QuestionnaireResponse, older_response=older_duplicate),
                    older_duplicate.classificationType != QuestionnaireResponseClassificationType.DUPLICATE
                )
            ).join(
                other_duplicate,
                self._responses_are_duplicates(QuestionnaireResponse, older_response=other_duplicate)
            ).outerjoin(
                newer_duplicate,
                self._responses_are_duplicates(newer_duplicate, older_response=QuestionnaireResponse)
            ).filter(
                # We should use the newest duplicate, and mark the older ones with classification DUPLICATE
                newer_duplicate.questionnaireResponseId.is_(None),
                QuestionnaireResponse.created >= earliest_response_date,
                # The Questionnaire id needs to be referenced to use the index that has the created date
                QuestionnaireResponse.questionnaireId > 0
            )
            .group_by(QuestionnaireResponse.questionnaireResponseId)
        ).all()

    @staticmethod
    def clean_pdr_module_data(duplicate_responses, session, project=GAE_PROJECT):
        """
        Delete PDR data records (pdr_mod_* table data) that are considered orphaned if the related
        questionnaire_response_id has been flagged as a duplicate.
        :param duplicate_responses:  List of questionnaire_response_id values that have been marked as DUPLICATE
        :param session:  A get_database().session() object
        :param project:  project name
        """
        # TODO:  Update to programmatically request deletion of PDR PostgreSQL records via pub/sub in the new
        # RDR-PDR pipeline.  Currently, corresponding records already populated over in PDR database(s) must be manually
        # deleted after this cleanup has run.  Can search the warning message logged in the flag_duplicate_responses()
        # method to get the pk_id values and run similar DELETE statements to the one here for the RDR
        # bigquery_sync table

        if not session:
            raise RuntimeError('Must supply an active session object to perform database operations')

        # Delete any records for pdr_mod_* tables that have a pk_id (questionnaire_response_id) that
        # has been marked as duplicate.  Limit how many records are being deleted per commit and inject a brief delay
        # between commits to avoid potential blocks in the database.
        pdr_project_id, pdr_dataset, _ = BQTable.get_project_map(project)[0]
        for pk_ids in list_chunks(duplicate_responses, 100):
            session.query(BigQuerySync
                          ).filter(BigQuerySync.projectId == pdr_project_id,
                                   BigQuerySync.datasetId == pdr_dataset,
                                   BigQuerySync.tableId.like('pdr_mod_%')
                          ).filter(BigQuerySync.pk_id.in_(pk_ids)).delete(synchronize_session=False)

            session.commit()
            sleep(0.25)

        participants = session.query(QuestionnaireResponse.participantId
                    ).filter(QuestionnaireResponse.questionnaireResponseId.in_(duplicate_responses)
                    ).group_by(QuestionnaireResponse.participantId
                    ).all()
        pid_list = [{'pid': p.participantId} for p in participants]
        task = GCPCloudTask()
        for batch in list_chunks(pid_list, 100):
            # Just want to rebuild the participant summary data (not the full modules), to remove remaining references
            # to the newly flagged duplicate responses from the participant summary / participant_module nested data
            payload = {'build_modules': False, 'batch': batch}
            if project == 'localhost':    # e.g., unittest case
                batch_rebuild_participants_task(payload)
            else:
                task.execute('rebuild_participants_task', payload=payload, project_id=project,
                             queue='resource-rebuild', in_seconds=30, quiet=True)

    def flag_duplicate_responses(self, num_days_ago=2, from_ts=datetime.utcnow()):
        """
        Search for duplicate questionnaire_responses created within the specified date range
        By default, will analyze responses created within the last two days from now
        :param from_ts:   datetime that establishes the end of the date range to search.  Default is utcnow()
        :param num_days_ago:  Number of days back from the from_ts to establish the start of the date range
        """
        earliest_response_date = from_ts - timedelta(days=num_days_ago)

        with get_database().session() as session:
            duplicated_response_data = self._get_duplicate_responses(session, earliest_response_date)
            questionnaire_ids_to_mark_as_duplicates = []
            for latest_duplicate_response_id, previous_duplicate_ids_str, duplication_count in duplicated_response_data:
                duplicates_needed = self.duplication_threshold - 1
                if duplication_count >= duplicates_needed:  # duplicate_count doesn't count the latest response
                    previous_duplicate_ids = previous_duplicate_ids_str.split(',')

                    logging.warning(f'{previous_duplicate_ids} found as duplicates of {latest_duplicate_response_id}')

                    questionnaire_ids_to_mark_as_duplicates.extend(previous_duplicate_ids)

            if questionnaire_ids_to_mark_as_duplicates:
                session.execute(
                    update(QuestionnaireResponse)
                    .where(QuestionnaireResponse.questionnaireResponseId.in_(questionnaire_ids_to_mark_as_duplicates))
                    .values({
                        QuestionnaireResponse.classificationType: QuestionnaireResponseClassificationType.DUPLICATE
                    })
                )
                self.clean_pdr_module_data(questionnaire_ids_to_mark_as_duplicates, session=session)
