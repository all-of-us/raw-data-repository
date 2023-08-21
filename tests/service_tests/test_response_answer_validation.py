import mock

from rdr_service.model.survey import Survey, SurveyQuestion
from rdr_service.services.response_validation.validation import ResponseValidator
from tests.helpers.unittest_base import BaseTestCase


class ResponseAnswerValidationTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uses_database = False

    def test_one_decimal_numbers(self):
        """Verify validation of redcap's number_1dp data type option"""
        validator = ResponseValidator(
            survey_definition=Survey(
                questions=[

                ]
            ),
            session=mock.MagicMock()
        )
