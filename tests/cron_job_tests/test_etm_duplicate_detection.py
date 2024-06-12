from tests.helpers.unittest_base import BaseTestCase
from tests.test_data import data_path
import json

from rdr_service.offline.etm_duplicate_detector import EtmDuplicateDetector
from rdr_service.model.etm import EtmQuestionnaireResponse
from rdr_service.participant_enums import QuestionnaireResponseClassificationType


class EtmDuplicateDetectionTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        with open(data_path("etm_questionnaire.json")) as file:
            questionnaire_json = json.load(file)
            self.send_post("Questionnaire", questionnaire_json)

        participant = self.data_generator.create_database_participant_summary()
        participant2 = self.data_generator.create_database_participant_summary()
        with open(data_path("etm_questionnaire_response.json")) as file:
            questionnaire_response_json = json.load(file)
        questionnaire_response_json["subject"][
            "reference"
        ] = f"Patient/P{participant.participantId}"
        self.send_post(
            f"Participant/P{participant.participantId}/QuestionnaireResponse",
            questionnaire_response_json,
        )
        self.send_post(
            f"Participant/P{participant.participantId}/QuestionnaireResponse",
            questionnaire_response_json,
        )

        questionnaire_response_json["subject"][
            "reference"
        ] = f"Patient/P{participant2.participantId}"
        self.send_post(
            f"Participant/P{participant2.participantId}/QuestionnaireResponse",
            questionnaire_response_json,
        )
        self.send_post(
            f"Participant/P{participant2.participantId}/QuestionnaireResponse",
            questionnaire_response_json,
        )
        self.send_post(
            f"Participant/P{participant2.participantId}/QuestionnaireResponse",
            questionnaire_response_json,
        )

    def test_etm_duplicate_detection(self):
        etm_duplicate_detector = EtmDuplicateDetector()
        duplicate_ids = etm_duplicate_detector.get_duplicate_ids(self.session)
        self.assertEqual(len(duplicate_ids), 3)

        etm_duplicate_detector.mark_responses_duplicate(duplicate_ids, self.session)

        for duplicate in duplicate_ids:
            marked_result = self.session.query(EtmQuestionnaireResponse).filter(
                EtmQuestionnaireResponse.etm_questionnaire_response_id == duplicate
            )

            self.assertEqual(
                marked_result[0].classificationType,
                QuestionnaireResponseClassificationType.DUPLICATE,
            )

        second_duplicate_id = etm_duplicate_detector.get_duplicate_ids(self.session)
        self.assertEqual(second_duplicate_id, [])
