from dataclasses import dataclass
from datetime import datetime
import json
from typing import List, Optional, Union

from rdr_service.domain_model.etm import EtmResponseAnswer
from rdr_service.model import etm
from rdr_service.participant_enums import QuestionnaireStatus
from tests.helpers.unittest_base import BaseTestCase
from tests.test_data import data_path


@dataclass
class EtmExtension:
    type: etm.ExtensionType
    key: str
    value: Optional[Union[str, int, float]]


class EtmIngestionTest(BaseTestCase):
    def test_questionnaire_ingestion(self):
        with open(data_path('etm_questionnaire.json')) as file:
            questionnaire_json = json.load(file)
        response = self.send_post('Questionnaire', questionnaire_json)

        questionnaire_obj: etm.EtmQuestionnaire = self.session.query(etm.EtmQuestionnaire).filter(
            etm.EtmQuestionnaire.etm_questionnaire_id == response['id']
        ).one()
        self.assertEqual(questionnaire_json['id'], questionnaire_obj.questionnaire_type)
        self.assertEqual(questionnaire_json['version'], questionnaire_obj.semantic_version)
        self.assertEqual(questionnaire_json['text']['div'], questionnaire_obj.title)

    def test_questionnaire_versioning(self):
        """Two questionnaires of the same type should receive different version numbers"""

        with open(data_path('etm_questionnaire.json')) as file:
            questionnaire_json = json.load(file)

        first_response = self.send_post('Questionnaire', questionnaire_json)
        second_response = self.send_post('Questionnaire', questionnaire_json)

        first_questionnaire: etm.EtmQuestionnaire = self.session.query(etm.EtmQuestionnaire).filter(
            etm.EtmQuestionnaire.etm_questionnaire_id == first_response['id']
        ).one()
        self.assertEqual(1, first_questionnaire.version)

        second_questionnaire: etm.EtmQuestionnaire = self.session.query(etm.EtmQuestionnaire).filter(
            etm.EtmQuestionnaire.etm_questionnaire_id == second_response['id']
        ).one()
        self.assertEqual(2, second_questionnaire.version)

    def test_questionnaire_response_ingestion(self):
        with open(data_path('etm_questionnaire.json')) as file:
            questionnaire_json = json.load(file)
            self.send_post('Questionnaire', questionnaire_json)

        participant_id = self.data_generator.create_database_participant().participantId

        with open(data_path('etm_questionnaire_response.json')) as file:
            questionnaire_response_json = json.load(file)
        questionnaire_response_json['subject']['reference'] = f'Patient/P{participant_id}'
        response = self.send_post(f'Participant/P{participant_id}/QuestionnaireResponse', questionnaire_response_json)

        saved_response: etm.EtmQuestionnaireResponse = self.session.query(etm.EtmQuestionnaireResponse).filter(
            etm.EtmQuestionnaireResponse.etm_questionnaire_response_id == response['id']
        ).one()
        self.assertEqual(datetime(2022, 11, 28, 20, 29, 43), saved_response.authored)
        self.assertEqual('emorecog', saved_response.questionnaire_type)
        self.assertEqual(QuestionnaireStatus.SUBMITTED, saved_response.status)
        self.assertEqual(participant_id, saved_response.participant_id)
        self.assertEqual(questionnaire_response_json, saved_response.resource)
        self.assert_has_answers(
            expected_answer_list=[
                EtmResponseAnswer(link_id=f'1.{index}', answer='f')
                for index in range(1, 49)
            ],
            actual_answer_list=saved_response.answer_list
        )
        self.assert_has_extensions(
            expected_list=[
                EtmExtension(type=etm.ExtensionType.METADATA, key='response_device', value='mouse'),
                EtmExtension(type=etm.ExtensionType.METADATA, key='user_agent', value='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'),
                EtmExtension(type=etm.ExtensionType.METADATA, key='screen_height', value=1117),
                EtmExtension(type=etm.ExtensionType.METADATA, key='screen_width', value=1728),
                EtmExtension(type=etm.ExtensionType.METADATA, key='touch', value=0),
                EtmExtension(type=etm.ExtensionType.METADATA, key='operating_system', value='MacOS'),
                EtmExtension(type=etm.ExtensionType.METADATA, key='user_utc_offset', value=-300),
                EtmExtension(type=etm.ExtensionType.METADATA, key='test_start_date_time', value=1669667302157),
                EtmExtension(type=etm.ExtensionType.METADATA, key='test_end_date_time', value=1669667382517),
                EtmExtension(type=etm.ExtensionType.METADATA, key='test_duration', value=80360),
                EtmExtension(type=etm.ExtensionType.METADATA, key='test_restarted', value=0),
                EtmExtension(type=etm.ExtensionType.METADATA, key='test_version', value='EmoRecog_AoU.v1.Nov22'),
                EtmExtension(type=etm.ExtensionType.METADATA, key='test_name', value='TMB Multiracial Emotion Identification'),
                EtmExtension(type=etm.ExtensionType.METADATA, key='test_short_name', value='emorecog'),
                EtmExtension(type=etm.ExtensionType.METADATA, key='test_params', value='{}'),
                EtmExtension(type=etm.ExtensionType.METADATA, key='test_language', value='en'),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='score', value=12),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='accuracy', value=0.25),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='meanRTc', value=219.69),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='medianRTc', value=176.65),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='sdRTc', value=159.53),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='happy_accuracy', value=0),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='happy_meanRTc', value=None),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='happy_medianRTc', value=None),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='happy_sdRTc', value=None),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='angry_accuracy', value=0),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='angry_meanRTc', value=None),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='angry_medianRTc', value=None),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='angry_sdRTc', value=None),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='fearful_accuracy', value=1),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='fearful_meanRTc', value=219.69),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='fearful_medianRTc', value=176.65),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='fearful_sdRTc', value=159.53),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='sad_accuracy', value=0),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='sad_meanRTc', value=None),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='sad_medianRTc', value=None),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='sad_sdRTc', value=None),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='any_timeouts', value=0),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='flag_medianRTc', value=1),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='flag_sameResponse', value=1),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='flag_trialFlags', value=1)
            ],
            actual_list=saved_response.extension_list
        )

    def assert_has_extensions(
        self,
        expected_list: List[EtmExtension],
        actual_list: List[etm.EtmQuestionnaireResponseMetadata]
    ):
        self.assertEqual(len(expected_list), len(actual_list))

        for expected in expected_list:
            found_match = False
            for actual in actual_list:
                if actual.value_int is not None:
                    actual_value = actual.value_int
                elif actual.value_decimal is not None:
                    actual_value = actual.value_decimal
                else:
                    actual_value = actual.value_string
                if (
                    actual.extension_type == expected.type
                    and actual.key == expected.key
                    and actual_value == expected.value
                ):
                    found_match = True
                    break

            if not found_match:
                self.fail(f'No match found for {expected}')

    def assert_has_answers(
        self,
        expected_answer_list: List[EtmResponseAnswer],
        actual_answer_list: List[etm.EtmQuestionnaireResponseAnswer]
    ):
        self.assertEqual(len(expected_answer_list), len(actual_answer_list))

        for expected_answer in expected_answer_list:
            found_match = False
            for actual_answer in actual_answer_list:
                if (
                    actual_answer.link_id == expected_answer.link_id
                    and actual_answer.answer_value == expected_answer.answer
                ):
                    found_match = True
                    break

            if not found_match:
                self.fail(f'No matching answer found for {expected_answer}')
