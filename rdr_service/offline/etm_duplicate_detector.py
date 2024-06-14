from typing import List
import os, logging

from sqlalchemy.orm import aliased
from sqlalchemy import update, or_

from rdr_service.config import GAE_PROJECT
from rdr_service.cloud_utils.gcp_google_pubsub import submit_pipeline_pubsub_msg
from rdr_service.dao.database_factory import get_database
from rdr_service.model.etm import EtmQuestionnaireResponse
from rdr_service.participant_enums import QuestionnaireResponseClassificationType


class EtmDuplicateDetector:
    def run(self):
        with get_database().session() as session:
            duplicate_ids = self.get_duplicate_ids(session)
            self.mark_responses_duplicate(duplicate_ids, session)
            self.clean_pdr_module_data(duplicate_ids)

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
    def clean_pdr_module_data(duplicate_responses: List[int], project=GAE_PROJECT) -> None:
        """
        Delete PDR data records (pdr_mod_* table data) that are considered orphaned if the related
        etm_questionnaire_response_id has been flagged as a duplicate.
        :param duplicate_responses:  List of etm_questionnaire_response_id values that have been marked as DUPLICATE
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


def run_etm_duplicate_detector():
    duplicate_detector = EtmDuplicateDetector()
    duplicate_detector.run()
