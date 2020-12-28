from datetime import datetime
import mock

from rdr_service.etl.model.src_clean import SrcClean
from rdr_service.model.participant import Participant
from rdr_service.tools.tool_libs.curation import CurationExportClass
from tests.helpers.unittest_base import BaseTestCase


class CurationEtlTest(BaseTestCase):
    def setUp(self):
        super(CurationEtlTest, self).setUp()
        self._setup_data()

    def _setup_data(self):
        self.participant = self.data_generator.create_database_participant()

        module_code = self.data_generator.create_database_code(value='src_clean_test')
        question_code = self.data_generator.create_database_code(value='q_1')

        self.questionnaire = self.data_generator.create_database_questionnaire_history(
            resource='''{
                        "identifier": [
                            {"value": "form_id_1"}
                        ]
                    }'''
        )
        self.data_generator.create_database_questionnaire_question(
            questionnaireId=self.questionnaire.questionnaireId,
            questionnaireVersion=self.questionnaire.version,
            codeId=question_code.codeId
        )

        self.data_generator.create_database_questionnaire_concept(
            questionnaireId=self.questionnaire.questionnaireId,
            questionnaireVersion=self.questionnaire.version,
            codeId=module_code.codeId
        )

        self.questionnaire_response = self.setup_questionnaire_response(self.participant, self.questionnaire)

    def setup_questionnaire_response(self, participant, questionnaire, authored=datetime(2020, 3, 15),
                                     created=datetime(2020, 3, 15)):
        questionnaire_response = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            questionnaireId=questionnaire.questionnaireId,
            questionnaireVersion=questionnaire.version,
            authored=authored,
            created=created
        )
        for question in questionnaire.questions:
            self.data_generator.create_database_questionnaire_response_answer(
                questionnaireResponseId=questionnaire_response.questionnaireResponseId,
                questionId=question.questionnaireQuestionId,
                valueString='test answer'
            )

        return questionnaire_response

    @staticmethod
    def run_tool():
        gcp_env = mock.MagicMock()

        args = mock.MagicMock()
        args.command = 'cdm-data'

        cope_answer_tool = CurationExportClass(args, gcp_env)
        cope_answer_tool.run()

    def test_locking(self):
        """Make sure that building the CDM tables doesn't take exclusive locks"""

        # Take an exclusive lock on the participant, one of the records known to be part of the insert query
        self.session.query(Participant).filter(
            Participant.participantId == self.participant.participantId
        ).with_for_update().one()

        # This will time out if the tool tries to take an exclusive lock on the participant
        self.run_tool()

    def _src_clean_record_found_for_response(self, questionnaire_response_id):
        response_record = self.session.query(SrcClean).filter(
            SrcClean.questionnaire_response_id == questionnaire_response_id
        ).one_or_none()
        return response_record is not None

    def test_latest_questionnaire_response_used(self):
        """The latest questionnaire response received for a module should be used"""
        # Note: this only applies to modules that shouldn't roll up answers (ConsentPII should be rolled up)

        # Create a questionnaire response that would be used instead of the default for the test suite
        later_response = self.setup_questionnaire_response(
            self.participant,
            self.questionnaire,
            authored=datetime(2020, 5, 10),
            created=datetime(2020, 5, 10)
        )

        # Check that the later response is used and that the previous response doesn't make it into the src_clean table
        self.run_tool()
        self.assertTrue(
            self._src_clean_record_found_for_response(later_response.questionnaireResponseId),
            'A src_clean record should be created for the later response'
        )
        self.assertFalse(
            self._src_clean_record_found_for_response(self.questionnaire_response.questionnaireResponseId),
            'A src_clean record should not be created for the earlier response'
        )

        # Check that two responses with the same authored date uses the latest one received (latest created date)
        latest_received_response = self.setup_questionnaire_response(
            self.participant,
            self.questionnaire,
            authored=later_response.authored,
            created=datetime(2020, 8, 1)
        )
        self.run_tool()
        self.assertTrue(
            self._src_clean_record_found_for_response(latest_received_response.questionnaireResponseId)
        )
        self.assertFalse(
            self._src_clean_record_found_for_response(later_response.questionnaireResponseId)
        )
        self.assertFalse(
            self._src_clean_record_found_for_response(self.questionnaire_response.questionnaireResponseId)
        )
