from datetime import datetime
import mock

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

        questionnaire = self.data_generator.create_database_questionnaire_history(
            resource='''{
                        "identifier": [
                            {"value": "form_id_1"}
                        ]
                    }'''
        )
        questionnaire_question = self.data_generator.create_database_questionnaire_question(
            questionnaireId=questionnaire.questionnaireId,
            questionnaireVersion=questionnaire.version,
            codeId=question_code.codeId
        )

        self.data_generator.create_database_questionnaire_concept(
            questionnaireId=questionnaire.questionnaireId,
            questionnaireVersion=questionnaire.version,
            codeId=module_code.codeId
        )

        questionnaire_response = self.data_generator.create_database_questionnaire_response(
            participantId=self.participant.participantId,
            questionnaireId=questionnaire.questionnaireId,
            questionnaireVersion=questionnaire.version,
            authored=datetime.now()
        )
        self.data_generator.create_database_questionnaire_response_answer(
            questionnaireResponseId=questionnaire_response.questionnaireResponseId,
            questionId=questionnaire_question.questionnaireQuestionId,
            valueString='test answer'
        )

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



