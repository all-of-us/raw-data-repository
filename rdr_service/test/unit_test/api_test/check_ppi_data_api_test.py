from rdr_service.api import check_ppi_data_api
from rdr_service.code_constants import FIRST_NAME_QUESTION_CODE
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.participant import Participant
from rdr_service.model.utils import to_client_participant_id
from rdr_service.test.test_data import email_code, first_name_code
from rdr_service.test.unit_test.unit_test_util import FlaskTestBase


# TODO: represent in new test suite
class CheckPpiDataApiTest(FlaskTestBase):
    def setUp(self):
        super(CheckPpiDataApiTest, self).setUp()
        CodeDao().insert(email_code())
        CodeDao().insert(first_name_code())

    def test_empty_request(self):
        response = self.send_post("CheckPpiData", {"ppi_data": {}})
        self.assertEqual({"ppi_results": {}}, response)

    def test_result_to_json(self):
        result = check_ppi_data_api._ValidationResult()
        result.add_error("ez")
        result.add_error("ea")
        result.tests_count += 11
        self.assertEqual({"tests_count": 11, "errors_count": 2, "error_messages": ["ez", "ea"]}, result.to_json())

    def test_validation_no_answer(self):
        self.participant = Participant(participantId=123, biobankId=555)
        ParticipantDao().insert(self.participant)
        self.participant_id = to_client_participant_id(self.participant.participantId)
        summary = ParticipantSummaryDao().insert(self.participant_summary(self.participant))

        result = check_ppi_data_api._get_validation_result(summary.email, {FIRST_NAME_QUESTION_CODE: "NotAnswered"})
        self.assertEqual(1, result.tests_count)
        self.assertEqual(1, result.errors_count)
        self.assertEqual(1, len(result.messages))
        self.assertIn(FIRST_NAME_QUESTION_CODE, result.messages[0])

        # test using phone number as lookup value in API.
        summary.loginPhoneNumber = "5555555555"
        ParticipantSummaryDao().update(summary)
        result = check_ppi_data_api._get_validation_result(
            summary.loginPhoneNumber, {FIRST_NAME_QUESTION_CODE: "NotAnswered"}
        )
        self.assertEqual(1, result.tests_count)
        self.assertEqual(1, result.errors_count)
        self.assertEqual(1, len(result.messages))
        self.assertIn(FIRST_NAME_QUESTION_CODE, result.messages[0])
