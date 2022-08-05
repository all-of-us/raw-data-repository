from datetime import date, datetime

from rdr_service.model.consent_file import ConsentFile
from rdr_service.model.consent_response import ConsentResponse
from rdr_service.model.questionnaire_response import QuestionnaireResponse
from rdr_service.tools.tool_libs.consent_response_matching import ConsentMatchScript
from tests.helpers.unittest_base import BaseTestCase


class ConsentResponseMatchingTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uses_database = False

    def test_one_to_one_matching(self):
        """If there's only one in each list, they're matched with each other"""
        consent_response = ConsentResponse()
        file = ConsentFile()

        ConsentMatchScript.match_files_to_responses(
            file_list=[file],
            consent_response_list=[consent_response]
        )
        self.assertEqual(consent_response, file.consent_response)

    def test_match_by_file_path(self):
        """A file can be matched up if it's file path is in the response payload"""
        response_file_paths = [
            'test_one.pdf',
            'aeouaoeu.pdf',
            'match.pdf',
            'other_file.pdf',
        ]
        consent_response_list = [
            ConsentResponse(
                response=QuestionnaireResponse(
                    resource=f"aoeuaoeu': '{file_path}'"
                )
            ) for file_path in response_file_paths
        ]
        file = ConsentFile(file_path='test_bucket_name/Participants/match.pdf')

        ConsentMatchScript.match_files_to_responses(
            file_list=[file],
            consent_response_list=consent_response_list
        )
        self.assertEqual(consent_response_list[2], file.consent_response)

    def test_match_by_authored_date(self):
        """If the file path isn't found, then the match should be based on the authored date"""
        response_authored_date_list = [
            datetime(2022, 1, 17),
            datetime(2022, 2, 14),
            datetime(2022, 5, 22),
            datetime(2022, 8, 19),
        ]
        consent_response_list = [
            ConsentResponse(
                response=QuestionnaireResponse(
                    authored=authored_date,
                    resource='necessary fluff'
                )
            ) for authored_date in response_authored_date_list
        ]
        file = ConsentFile(expected_sign_date=date(2022, 2, 14), file_path='test_bucket_name/Participants/test.pdf')

        ConsentMatchScript.match_files_to_responses(
            file_list=[file],
            consent_response_list=consent_response_list
        )
        self.assertEqual(consent_response_list[1], file.consent_response)

    def test_multiple_match_to_different_responses(self):
        """The matching algorithm should try to match one validation record to one consent_response"""
        response_authored_date_list = [
            datetime(2022, 2, 14),
            datetime(2022, 2, 14)
        ]
        consent_response_list = [
            ConsentResponse(
                response=QuestionnaireResponse(
                    authored=authored_date,
                    resource='necessary fluff'
                )
            ) for authored_date in response_authored_date_list
        ]
        first_file = ConsentFile(
            expected_sign_date=date(2022, 2, 14),
            file_path='test_bucket_name/Participants/test.pdf'
        )
        second_file = ConsentFile(
            expected_sign_date=date(2022, 2, 14),
            file_path='test_bucket_name/Participants/other.pdf'
        )

        ConsentMatchScript.match_files_to_responses(
            file_list=[first_file, second_file],
            consent_response_list=consent_response_list
        )
        self.assertEqual(consent_response_list[0], first_file.consent_response)
        self.assertEqual(consent_response_list[1], second_file.consent_response)
