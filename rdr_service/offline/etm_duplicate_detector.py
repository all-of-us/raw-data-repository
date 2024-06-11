from typing import List
import os, logging
from time import sleep

from sqlalchemy.orm import aliased
from sqlalchemy import update, or_

from rdr_service.config import GAE_PROJECT
from rdr_service.cloud_utils.gcp_google_pubsub import submit_pipeline_pubsub_msg
from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask
from rdr_service.dao.database_factory import get_database
from rdr_service.model.etm import EtmQuestionnaireResponse
from rdr_service.participant_enums import QuestionnaireResponseClassificationType
from rdr_service.model.bigquery_sync import BigQuerySync
from rdr_service.model.bq_base import BQTable
from rdr_service.resource.tasks import batch_rebuild_participants_task
from rdr_service.services.system_utils import list_chunks




class EtmDuplicateDetector:
    def run(self):
        with get_database().session() as session:
            duplicate_ids = self.get_duplicate_ids(session)
            self.mark_responses_duplicate(duplicate_ids, session)
            self.clean_pdr_module_data(duplicate_ids, session)

    @staticmethod
    def get_duplicate_ids(session) -> List[int]:
        duplicate_qr = aliased(EtmQuestionnaireResponse)
        duplicates = (
            session.query(duplicate_qr.etm_questionnaire_response_id)
            .select_from(EtmQuestionnaireResponse)
            .join(
                duplicate_qr,
                duplicate_qr.response_hash
                == EtmQuestionnaireResponse.response_hash,
            )
            .filter(
                EtmQuestionnaireResponse.etm_questionnaire_response_id
                != duplicate_qr.etm_questionnaire_response_id,
                or_(
                    EtmQuestionnaireResponse.created < duplicate_qr.created,
                    EtmQuestionnaireResponse.etm_questionnaire_response_id < duplicate_qr.etm_questionnaire_response_id,
                ),
                EtmQuestionnaireResponse.classificationType
                == QuestionnaireResponseClassificationType.COMPLETE,
                duplicate_qr.classificationType
                == QuestionnaireResponseClassificationType.COMPLETE,
            )
        ).distinct()
        duplicate_ids = [d_id[0] for d_id in duplicates.all()]
        return duplicate_ids

    @staticmethod
    def mark_responses_duplicate(duplicate_ids: List[int], session) -> None:
        """Sets the classification type for etm_questionnaire_response_ids in `duplicate_ids` as DUPLICATE"""
        session.execute(
            update(EtmQuestionnaireResponse)
            .where(
                EtmQuestionnaireResponse.etm_questionnaire_response_id.in_(duplicate_ids)
            )
            .values({
                    EtmQuestionnaireResponse.classificationType: QuestionnaireResponseClassificationType.DUPLICATE
                    })
        )
        session.commit()

    @staticmethod
    def clean_pdr_module_data(duplicate_responses: List[int], session, project=GAE_PROJECT) -> None:
        """
        Delete PDR data records (pdr_mod_* table data) that are considered orphaned if the related
        etm_questionnaire_response_id has been flagged as a duplicate.
        :param duplicate_responses:  List of etm_questionnaire_response_id values that have been marked as DUPLICATE
        :param session:  A get_database().session() object
        :param project:  project name
        """

        # For new RDR-PDR pipeline:  generate PDR delete record events for the marked duplicates.  Allow calls
        # during unittests for mocks/param validation.  submit_pipeline_pubsub_msg() will enforce project restrictions
        if project != "localhost" or os.environ.get("UNITTEST_FLAG", "0") == "1":
            submit_pipeline_pubsub_msg(
                table="etm_questionnaire_response",
                action="delete",
                pk_columns=["etm_questionnaire_response_id"],
                pk_values=duplicate_responses,
                project=project,
            )

            logging.info(
                f"Sent PubSub notifications to mark {len(duplicate_responses)} records for deletion."
            )

        # TODO: Once the new pipeline is fully operational, the following code can be deleted:
        task = GCPCloudTask()
        if not session:
            raise RuntimeError(
                "Must supply an active session object to perform database operations"
            )

        # Delete any records for pdr_mod_* tables that have a pk_id (questionnaire_response_id) that
        # has been marked as duplicate.  Limit how many records are being deleted per commit and inject a brief delay
        # between commits to avoid potential blocks in the database.
        pdr_project_id, pdr_dataset, _ = BQTable.get_project_map(project)[0]
        for pk_ids in list_chunks(duplicate_responses, 100):
            session.query(BigQuerySync).filter(
                BigQuerySync.projectId == pdr_project_id,
                BigQuerySync.datasetId == pdr_dataset,
                BigQuerySync.tableId.like("pdr_mod_%"),
            ).filter(BigQuerySync.pk_id.in_(pk_ids)).delete(synchronize_session=False)

            session.commit()
            sleep(0.25)

        participants = (
            session.query(EtmQuestionnaireResponse.participant_id)
            .filter(
                EtmQuestionnaireResponse.etm_questionnaire_response_id.in_(duplicate_responses)
            )
            .group_by(EtmQuestionnaireResponse.participant_id)
            .all()
        )
        pid_list = [{"pid": p.participant_id} for p in participants]

        for batch in list_chunks(pid_list, 100):
            # Just want to rebuild the participant summary data (not the full modules), to remove remaining references
            # to the newly flagged duplicate responses from the participant summary / participant_module nested data
            payload = {"build_modules": False, "batch": batch}
            if project == "localhost":  # e.g., unittest case
                batch_rebuild_participants_task(payload)
            else:
                task.execute(
                    "rebuild_participants_task",

                    payload=payload,
                    project_id=project,
                    queue="resource-rebuild",
                    in_seconds=30,
                    quiet=True,
                )


def run_etm_duplicate_detector():
    duplicate_detector = EtmDuplicateDetector()
    duplicate_detector.run()
