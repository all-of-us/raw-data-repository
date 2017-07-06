from api import check_ppi_data_api
from code_constants import FIRST_NAME_QUESTION_CODE
from dao.code_dao import CodeDao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.utils import to_client_participant_id
from model.participant import Participant
from test.test_data import email_code, first_name_code
from test.unit_test.unit_test_util import FlaskTestBase


class CheckPpiDataApiTest(FlaskTestBase):
  def setUp(self):
    super(CheckPpiDataApiTest, self).setUp()
    CodeDao().insert(email_code())
    CodeDao().insert(first_name_code())

  def test_empty_request(self):
    response = self.send_post('CheckPpiData', {'ppi_data': {}})
    self.assertEquals({'ppi_results': {}}, response)

  def test_result_to_json(self):
    result = check_ppi_data_api._ValidationResult()
    result.add_error('ez')
    result.add_error('ea')
    result.tests_count += 11
    self.assertEquals(
        {'tests_count': 11, 'errors_count': 2, 'messages': ['ez', 'ea']},
        result.to_json())

  def test_validation_no_answer(self):
    self.participant = Participant(participantId=123, biobankId=555)
    ParticipantDao().insert(self.participant)
    self.participant_id = to_client_participant_id(self.participant.participantId)
    summary = ParticipantSummaryDao().insert(self.participant_summary(self.participant))

    result = check_ppi_data_api._get_validation_result(
        summary.email, {FIRST_NAME_QUESTION_CODE: 'NotAnswered'})
    self.assertEquals(1, result.tests_count)
    self.assertEquals(1, result.errors_count)
    self.assertEquals(1, len(result.messages))
    self.assertIn(FIRST_NAME_QUESTION_CODE, result.messages[0])
