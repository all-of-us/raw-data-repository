from dataclasses import dataclass
from datetime import datetime, timedelta
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
        self.send_post('Questionnaire', questionnaire_json)

        questionnaire_obj: etm.EtmQuestionnaire = self.session.query(etm.EtmQuestionnaire).one()
        self.assertEqual('emorecog', questionnaire_obj.questionnaire_type)
        self.assertEqual(questionnaire_json['identifier'][0]['value'], questionnaire_obj.semantic_version)
        self.assertEqual(questionnaire_json['text']['div'], questionnaire_obj.title)

    def test_questionnaire_versioning(self):
        """Two questionnaires of the same type should receive different version numbers"""

        with open(data_path('etm_questionnaire.json')) as file:
            questionnaire_json = json.load(file)

        self.send_post('Questionnaire', questionnaire_json)
        self.send_post('Questionnaire', questionnaire_json)

        questionnaire_list: List[etm.EtmQuestionnaire] = self.session.query(etm.EtmQuestionnaire).order_by(
            etm.EtmQuestionnaire.version
        ).all()
        for expected_version, questionnaire in enumerate(questionnaire_list, start=1):
            self.assertEqual(expected_version, questionnaire.version)

    def test_questionnaire_api_response(self):
        """Ensure that the full json sent is returned in the response"""
        with open(data_path('etm_questionnaire.json')) as file:
            questionnaire_json = json.load(file)
        response = self.send_post('Questionnaire', questionnaire_json)

        self.assertEqual(questionnaire_json, response)

    def test_questionnaire_response_ingestion(self):
        with open(data_path('etm_questionnaire.json')) as file:
            questionnaire_json = json.load(file)
            self.send_post('Questionnaire', questionnaire_json)

        participant = self.data_generator.create_database_participant()
        self.data_generator.create_database_participant_summary(participant=participant)
        with open(data_path('etm_questionnaire_response.json')) as file:
            questionnaire_response_json = json.load(file)
        questionnaire_response_json['subject']['reference'] = f'Patient/P{participant.participantId}'
        response = self.send_post(
            f'Participant/P{participant.participantId}/QuestionnaireResponse', questionnaire_response_json
        )

        saved_response: etm.EtmQuestionnaireResponse = self.session.query(etm.EtmQuestionnaireResponse).filter(
            etm.EtmQuestionnaireResponse.etm_questionnaire_response_id == response['id']
        ).one()
        self.assertEqual(datetime(2023, 6, 23, 11, 56, 30), saved_response.authored)
        self.assertEqual('emorecog', saved_response.questionnaire_type)
        self.assertEqual(QuestionnaireStatus.SUBMITTED, saved_response.status)
        self.assertEqual(participant.participantId, saved_response.participant_id)
        self.assertEqual(questionnaire_response_json, saved_response.resource)
        self.assert_has_answers(
            expected_answer_list=[
                EtmResponseAnswer(trial_id=f'test{index}', answer='f')
                for index in range(1, 49)
            ] + [EtmResponseAnswer(trial_id='test17', answer=None)],
            actual_answer_list=saved_response.answer_list
        )
        self.assert_has_extensions(
            expected_list=[
                EtmExtension(type=etm.ExtensionType.METADATA, key='response_device', value='mouse'),
                EtmExtension(type=etm.ExtensionType.METADATA, key='user_agent', value='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'),
                EtmExtension(type=etm.ExtensionType.METADATA, key='screen_height', value=1117),
                EtmExtension(type=etm.ExtensionType.METADATA, key='screen_width', value=1728),
                EtmExtension(type=etm.ExtensionType.METADATA, key='touch', value=0),
                EtmExtension(type=etm.ExtensionType.METADATA, key='operating_system', value='MacOS'),
                EtmExtension(type=etm.ExtensionType.METADATA, key='user_utc_offset', value=-300),
                EtmExtension(type=etm.ExtensionType.METADATA, key='test_start_date_time', value=1687521289096),
                EtmExtension(type=etm.ExtensionType.METADATA, key='test_end_date_time', value=1687521390759),
                EtmExtension(type=etm.ExtensionType.METADATA, key='test_duration', value=101663),
                EtmExtension(type=etm.ExtensionType.METADATA, key='test_restarted', value=0),
                EtmExtension(type=etm.ExtensionType.METADATA, key='test_version', value='EmoRecog_AoU.v1.May23'),
                EtmExtension(type=etm.ExtensionType.METADATA, key='test_name', value='TestMyBrain Multiracial Emotion Identification'),
                EtmExtension(type=etm.ExtensionType.METADATA, key='test_short_name', value='emorecog'),
                EtmExtension(type=etm.ExtensionType.METADATA, key='test_params', value='{}'),
                EtmExtension(type=etm.ExtensionType.METADATA, key='test_language', value='en'),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='score', value=13),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='accuracy', value=0.2708333333333333),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='meanRTc', value=380.18),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='medianRTc', value=302.6),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='sdRTc', value=310.4),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='happy_accuracy', value=0.08333333333333333),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='happy_meanRTc', value=264.9),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='happy_medianRTc', value=264.9),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='happy_sdRTc', value=None),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='angry_accuracy', value=0.25),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='angry_meanRTc', value=206.63),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='angry_medianRTc', value=253.5),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='angry_sdRTc', value=135.33),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='fearful_accuracy', value=0.5833333333333334),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='fearful_meanRTc', value=496.19),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='fearful_medianRTc', value=306.2),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='fearful_sdRTc', value=388.6),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='sad_accuracy', value=0.16666666666666666),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='sad_meanRTc', value=292.15),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='sad_medianRTc', value=292.15),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='sad_sdRTc', value=12.94),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='any_timeouts', value=0),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='flag_medianRTc', value=1),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='flag_sameResponse', value=0),
                EtmExtension(type=etm.ExtensionType.OUTCOME, key='flag_trialFlags', value=1)
            ],
            actual_list=saved_response.extension_list
        )

        # Confirm participant_summary record was updated with the task authored time
        summary = self.send_get(f"Participant/P{participant.participantId}/Summary")
        ps_etm_authored_str = summary.get('latestEtMTaskAuthored', None)
        self.assertIsNotNone(ps_etm_authored_str)
        ps_ts = datetime.strptime(ps_etm_authored_str, '%Y-%m-%dT%H:%M:%S')
        self.assertEqual(ps_ts, saved_response.authored)

    def test_multiple_etm_tasks_for_retention(self):
        with open(data_path('etm_questionnaire.json')) as file:
            questionnaire_json = json.load(file)
            self.send_post('Questionnaire', questionnaire_json)

        participant = self.data_generator.create_database_participant()
        self.data_generator.create_database_participant_summary(participant=participant)
        with open(data_path('etm_questionnaire_response.json')) as file:
            questionnaire_response_json = json.load(file)
        questionnaire_response_json['subject']['reference'] = f'Patient/P{participant.participantId}'
        resp_authored = datetime.fromisoformat(questionnaire_response_json['authored']).replace(tzinfo=None)
        # Creating 3 EtM task responses, confirming that we end up with the latest authored date in participant summary
        # even if it is not the last response ingested
        for etm_task_date in (resp_authored, resp_authored + timedelta(days=21), resp_authored + timedelta(days=10)):
            questionnaire_response_json['authored'] = etm_task_date.strftime('%Y-%m-%dT%H:%M:%S+00:00')
            self.send_post(
                f'Participant/P{participant.participantId}/QuestionnaireResponse', questionnaire_response_json
            )
        etm_records: etm.EtmQuestionnaireResponse = self.session.query(etm.EtmQuestionnaireResponse).all()
        # Confirm participant_summary record was updated with the task authored date
        summary = self.send_get(f"Participant/P{participant.participantId}/Summary")
        self.assertIsNotNone(summary)
        self.assertIsNotNone(summary.get('latestEtMTaskAuthored', None))
        ps_latest_etm_authored = datetime.fromisoformat(summary.get('latestEtMTaskAuthored'))
        self.assertEqual(resp_authored + timedelta(days=21), ps_latest_etm_authored)


    def test_questionnaire_response_api_response(self):
        """Check that the QuestionnaireResponse data is returned by the API"""
        with open(data_path('etm_questionnaire.json')) as file:
            questionnaire_json = json.load(file)
            self.send_post('Questionnaire', questionnaire_json)

        participant_id = self.data_generator.create_database_participant().participantId

        with open(data_path('etm_questionnaire_response.json')) as file:
            questionnaire_response_json = json.load(file)
        questionnaire_response_json['subject']['reference'] = f'Patient/P{participant_id}'
        response = self.send_post(f'Participant/P{participant_id}/QuestionnaireResponse', questionnaire_response_json)

        del response['id']
        self.assertEqual(questionnaire_response_json, response)

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
                ):
                    if isinstance(expected.value, float):
                        found_match = abs(float(actual_value) - expected.value) < 0.001
                    else:
                        found_match = actual_value == expected.value
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
                    actual_answer.trial_id == expected_answer.trial_id
                    and actual_answer.answer_value == expected_answer.answer
                ):
                    found_match = True
                    break

            if not found_match:
                self.fail(f'No matching answer found for {expected_answer}')
