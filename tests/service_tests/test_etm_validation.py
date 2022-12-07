
from rdr_service.domain_model import etm
from rdr_service.services.response_validation.etm_validation import EtmValidation
from tests.helpers.unittest_base import BaseTestCase


class EtmValidationTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uses_database = False

    def test_validating_metadata(self):
        questionnaire = etm.EtmQuestionnaire(
            metadata_name_list=[
                'touch',
                'user_utc_offset',
                'test_duration',
                'test_name'
            ]
        )
        response = etm.EtmResponse(
            metadata_list=[
                etm.EtmResponseExtension(key='touch'),
                etm.EtmResponseExtension(key='user_utc_offset')
            ]
        )

        result = EtmValidation.validate_response(response=response, questionnaire=questionnaire)
        self.assertFalse(result.success)
        self.assertEqual(
            [
                'Missing "test_duration" metadata field',
                'Missing "test_name" metadata field'
            ],
            result.errors
        )

        response.metadata_list.extend([
            etm.EtmResponseExtension(key='test_duration'),
            etm.EtmResponseExtension(key='test_name')
        ])
        result = EtmValidation.validate_response(response=response, questionnaire=questionnaire)
        self.assertTrue(result.success)
        self.assertEmpty(result.errors)

    def test_validating_outcomes(self):
        questionnaire = etm.EtmQuestionnaire(
            outcome_name_list=[
                'score',
                'accuracy',
                'meanRTc',
                'medianRTc'
            ]
        )
        response = etm.EtmResponse(
            outcome_list=[
                etm.EtmResponseExtension(key='score'),
                etm.EtmResponseExtension(key='meanRTc')
            ]
        )

        result = EtmValidation.validate_response(response=response, questionnaire=questionnaire)
        self.assertFalse(result.success)
        self.assertEqual(
            [
                'Missing "accuracy" outcome field',
                'Missing "medianRTc" outcome field'
            ],
            result.errors
        )

        response.outcome_list.extend([
            etm.EtmResponseExtension(key='accuracy'),
            etm.EtmResponseExtension(key='medianRTc')
        ])
        result = EtmValidation.validate_response(response=response, questionnaire=questionnaire)
        self.assertTrue(result.success)
        self.assertEmpty(result.errors)

    def test_answer_count_checking(self):
        questionnaire = etm.EtmQuestionnaire(
            question_list=[
                etm.EtmQuestion(link_id='1.1', required=True),
                etm.EtmQuestion(link_id='1.2', required=True),
                etm.EtmQuestion(link_id='1.3', required=True)
            ]
        )
        response = etm.EtmResponse(
            answer_list=[
                etm.EtmResponseAnswer(link_id='1.1'),
                etm.EtmResponseAnswer(link_id='1.3')
            ]
        )

        result = EtmValidation.validate_response(response=response, questionnaire=questionnaire)
        self.assertFalse(result.success)
        self.assertEqual(
            ['Missing answer for question "1.2"'],
            result.errors
        )

        response.answer_list.append(
            etm.EtmResponseAnswer(link_id='1.2')
        )
        result = EtmValidation.validate_response(response=response, questionnaire=questionnaire)
        self.assertTrue(result.success)
        self.assertEmpty(result.errors)
