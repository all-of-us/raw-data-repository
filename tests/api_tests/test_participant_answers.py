import http.client

from rdr_service.code_constants import PPI_SYSTEM
from rdr_service.concepts import Concept
from tests.helpers.unittest_base import BaseTestCase


class QuestionnaireAnswersApiTest(BaseTestCase):

    def setUp(self, with_data=True, with_consent_codes=True):
        super().setUp(with_data, with_consent_codes)

    def _answers_url(self, p_id, module):
        return "Participant/{0}/QuestionnaireAnswers/{1}".format(p_id, module)

    def test_valid_module_answers(self):

        p_id = self.create_participant()

        email = "answers@test.com"
        code_answers = [
            ("language", Concept(PPI_SYSTEM, "SpokenWrittenLanguage_English")),
            ("language", Concept(PPI_SYSTEM, "SpokenWrittenLanguage_ChineseChina")),
        ]
        self.send_consent(p_id, email=email, language="en", code_values=code_answers)

        response = self.send_get(self._answers_url(p_id, "ConsentPII"))

        self.assertIsNotNone(response)
        self.assertEqual(len(response), 1)

        answers = response[0]
        self.assertEqual(answers["ConsentPII_EmailAddress"], "answers@test.com")

        # verify multiple selection question, both languages should be in the response.
        self.assertIn("SpokenWrittenLanguage_English", answers["Language_SpokenWrittenLanguage"])
        self.assertIn("SpokenWrittenLanguage_ChineseChina", answers["Language_SpokenWrittenLanguage"])

    def test_module_not_answered(self):
        """
    Test an existing module, but no questionnaire response has been submitted.
    """
        p_id = self.create_participant()

        email = "answers@test.com"
        code_answers = [
            ("language", Concept(PPI_SYSTEM, "SpokenWrittenLanguage_English")),
            ("language", Concept(PPI_SYSTEM, "SpokenWrittenLanguage_ChineseChina")),
        ]
        self.send_consent(p_id, email=email, language="en", code_values=code_answers)

        self.send_get(self._answers_url(p_id, "OverallHealth"), expected_status=http.client.NOT_FOUND)

    def test_invalid_module(self):
        """
        Test an invalid module name.
        """
        p_id = self.create_participant()

        email = "answers@test.com"
        code_answers = [
            ("language", Concept(PPI_SYSTEM, "SpokenWrittenLanguage_English")),
            ("language", Concept(PPI_SYSTEM, "SpokenWrittenLanguage_ChineseChina")),
        ]
        self.send_consent(p_id, email=email, language="en", code_values=code_answers)

        self.send_get(self._answers_url(p_id, "InvalidModule"), expected_status=http.client.BAD_REQUEST)
