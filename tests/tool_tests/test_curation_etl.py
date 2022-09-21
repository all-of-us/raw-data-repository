from datetime import datetime, date
from typing import Collection

from rdr_service.code_constants import CONSENT_FOR_STUDY_ENROLLMENT_MODULE, EMPLOYMENT_ZIPCODE_QUESTION_CODE, PMI_SKIP_CODE,\
    STREET_ADDRESS_QUESTION_CODE, STREET_ADDRESS2_QUESTION_CODE, ZIPCODE_QUESTION_CODE
from rdr_service.etl.model.src_clean import SrcClean
from rdr_service.model.code import Code
from rdr_service.model.participant import Participant
from rdr_service.dao.curation_etl_dao import CdrEtlRunHistoryDao, CdrEtlSurveyHistoryDao
from rdr_service.participant_enums import QuestionnaireResponseStatus, QuestionnaireResponseClassificationType
from rdr_service.tools.tool_libs.curation import CurationExportClass
from tests.helpers.unittest_base import BaseTestCase
from tests.helpers.tool_test_mixin import ToolTestMixin
from rdr_service.clock import FakeClock

TIME = datetime(2000, 1, 10)
TIME_2 = datetime(2022, 5, 10)


class CurationEtlTest(ToolTestMixin, BaseTestCase):
    def setUp(self):
        super(CurationEtlTest, self).setUp(with_consent_codes=True)
        self._setup_data()

    def _setup_data(self):
        self.participant = self.data_generator.create_database_participant()
        self.data_generator.create_database_participant_summary(
            participant=self.participant,
            dateOfBirth=datetime(1982, 1, 9),
            consentForStudyEnrollmentFirstYesAuthored=datetime(2000, 1, 10))

        self.module_code = self.data_generator.create_database_code(value='src_clean_test')
        self.question_code_list = []
        self.indexed_answers = []
        self.questionnaire = self.data_generator.create_database_questionnaire_history()
        for question_index in range(4):
            question_code = self.data_generator.create_database_code(value=f'q_{question_index}')
            self.question_code_list.append(question_code)
            self.data_generator.create_database_questionnaire_question(
                questionnaireId=self.questionnaire.questionnaireId,
                questionnaireVersion=self.questionnaire.version,
                codeId=question_code.codeId
            )
            answer_code = self.data_generator.create_database_code(value=f'a_{question_index}')
            self.indexed_answers.append((question_index, 'valueCodeId', answer_code.codeId))

        self.data_generator.create_database_questionnaire_concept(
            questionnaireId=self.questionnaire.questionnaireId,
            questionnaireVersion=self.questionnaire.version,
            codeId=self.module_code.codeId
        )

        self.questionnaire_response = self._setup_questionnaire_response(self.participant, self.questionnaire,
                                                                         indexed_answers=self.indexed_answers)

    def _setup_questionnaire_response(self, participant, questionnaire, authored=datetime(2020, 3, 15),
                                      created=datetime(2020, 3, 15), indexed_answers=None, ignored_answer_indexes=None,
                                      status=QuestionnaireResponseStatus.COMPLETED,
                                      classification_type=QuestionnaireResponseClassificationType.COMPLETE):

        if not ignored_answer_indexes:
            ignored_answer_indexes = []

        questionnaire_response = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            questionnaireId=questionnaire.questionnaireId,
            questionnaireVersion=questionnaire.version,
            authored=authored,
            created=created,
            status=status,
            classificationType=classification_type
        )

        if indexed_answers is None:
            # If no answers were specified then answer all questions with 'test answer'
            indexed_answers = [
                (question_index, 'valueString', 'test answer')
                for question_index in range(len(questionnaire.questions))
            ]

        for question_index, answer_field_name, answer_string in indexed_answers:
            question = questionnaire.questions[question_index]
            self.data_generator.create_database_questionnaire_response_answer(
                questionnaireResponseId=questionnaire_response.questionnaireResponseId,
                questionId=question.questionnaireQuestionId,
                **{answer_field_name: answer_string},
                ignore=question_index in ignored_answer_indexes
            )

        return questionnaire_response

    @staticmethod
    def run_cdm_data_generation(cutoff=None, vocabulary='gs://curation-vocabulary/aou_vocab_20220201/'):
        CurationEtlTest.run_tool(CurationExportClass, tool_args={
            'command': 'cdm-data',
            'cutoff': cutoff,
            'vocabulary': vocabulary
        })

    @staticmethod
    def run_exclude_code_command(operation, code_value, code_type):
        CurationEtlTest.run_tool(CurationExportClass, tool_args={
            'command': 'exclude-code',
            'operation': operation,
            'code_value': code_value,
            'code_type': code_type
        })

    def test_locking(self):
        """Make sure that building the CDM tables doesn't take exclusive locks"""

        # Take an exclusive lock on the participant, one of the records known to be part of the insert query
        self.session.query(Participant).filter(
            Participant.participantId == self.participant.participantId
        ).with_for_update().one()

        # This will time out if the tool tries to take an exclusive lock on the participant
        self.run_cdm_data_generation()

    def _src_clean_record_found_for_response(self, questionnaire_response_id):
        response_record = self.session.query(SrcClean).filter(
            SrcClean.questionnaire_response_id == questionnaire_response_id
        ).one_or_none()
        return response_record is not None

    def test_latest_questionnaire_response_used(self):
        """The latest questionnaire response received for a module should be used"""
        # Note: this only applies to modules that shouldn't roll up answers (ConsentPII should be rolled up)

        # Create a questionnaire response that would be used instead of the default for the test suite
        self._setup_questionnaire_response(
            self.participant,
            self.questionnaire,
            indexed_answers=[
                (1, 'valueString', 'update'),
                (3, 'valueString', 'final answer')
            ],
            authored=datetime(2020, 5, 10),
            created=datetime(2020, 5, 10)
        )

        # Check that we are only be seeing the answers from the latest questionnaire response
        self.run_cdm_data_generation()
        for question_index, question in enumerate(self.questionnaire.questions):
            expected_answer = None
            if question_index == 1:
                expected_answer = 'update'
            elif question_index == 3:
                expected_answer = 'final answer'

            src_clean_answer = self.session.query(SrcClean).filter(
                SrcClean.question_code_id == question.codeId
            ).one_or_none()
            if expected_answer is None:
                self.assertIsNone(src_clean_answer)
            else:
                self.assertEqual(expected_answer, src_clean_answer.value_string)

    def _create_consent_questionnaire(self):
        module_code = self.session.query(Code).filter(Code.value == CONSENT_FOR_STUDY_ENROLLMENT_MODULE).one()
        consent_question_codes = [
            self.data_generator.create_database_code(value=f'consent_q_code_{question_index}')
            for question_index in range(4)
        ]
        consent_question_codes += self.session.query(Code).filter(Code.value.in_([
            STREET_ADDRESS_QUESTION_CODE, STREET_ADDRESS2_QUESTION_CODE
        ])).all()

        consent_questionnaire = self.data_generator.create_database_questionnaire_history()
        for consent_question_code in consent_question_codes:
            self.data_generator.create_database_questionnaire_question(
                questionnaireId=consent_questionnaire.questionnaireId,
                questionnaireVersion=consent_questionnaire.version,
                codeId=consent_question_code.codeId
            )

        self.data_generator.create_database_questionnaire_concept(
            questionnaireId=consent_questionnaire.questionnaireId,
            questionnaireVersion=consent_questionnaire.version,
            codeId=module_code.codeId
        )

        return consent_questionnaire

    def test_consent_response_answers_roll_up(self):
        """
        For the consent survey, all of the most recent answers for a code should be used
        even if they were in previous responses.
        """

        consent_questionnaire = self._create_consent_questionnaire()

        self._setup_questionnaire_response(self.participant, consent_questionnaire)
        self._setup_questionnaire_response(
            self.participant,
            consent_questionnaire,
            indexed_answers=[
                (1, 'valueString', 'NewLastName'),
                (3, 'valueString', 'new-email')
            ],
            authored=datetime(2020, 5, 1)
        )
        self._setup_questionnaire_response(
            self.participant,
            consent_questionnaire,
            indexed_answers=[
                (2, 'valueString', 'updated address'),
                (3, 'valueString', 'corrected-email')
            ],
            authored=datetime(2020, 8, 1)
        )

        # Check that the newest answer is in the src_clean, even if it wasn't from the latest response
        self.run_cdm_data_generation()
        for question_index, question in enumerate(consent_questionnaire.questions):
            expected_answer = 'test answer'
            if question_index == 1:
                expected_answer = 'NewLastName'
            elif question_index == 2:
                expected_answer = 'updated address'
            elif question_index == 3:
                expected_answer = 'corrected-email'

            # Since there was an initial response with an answer for every question, then every question
            # should have an answer in the export (even though partial responses updated some of them).
            # There also shouldn't be multiple answers from the participant for any of the survey
            # questions in the export.
            src_clean_answer_query = self.session.query(SrcClean).filter(
                SrcClean.question_code_id == question.codeId
            ).one()
            self.assertEqual(expected_answer, src_clean_answer_query.value_string)

    def test_consent_address_roll_up(self):
        """
        For the consent survey, any answers for the first line of the street address should also
        override previous answers for StreetAddress2
        """

        consent_questionnaire = self._create_consent_questionnaire()

        # Set up a response that answers all the questions, including the two address lines
        self._setup_questionnaire_response(self.participant, consent_questionnaire)
        self._setup_questionnaire_response(
            self.participant,
            consent_questionnaire,
            authored=datetime(2020, 5, 1)
        )

        # Enter another response that just updates the first line of the street address
        expected_final_address = '42 Wallaby Way'
        self._setup_questionnaire_response(
            self.participant,
            consent_questionnaire,
            indexed_answers=[
                (4, 'valueString', expected_final_address)  # Assuming the 4th question is the first line of the address
            ],
            authored=datetime(2020, 8, 1)
        )

        # Check that the only address answer in src_clean is the updated line 1 for the street address (which will
        # also replace line 2 for the the first response)
        self.run_cdm_data_generation()

        # Load all src_clean rows for lines 1 and 2 of the address
        address_answers = self.session.query(SrcClean).join(
            Code, Code.codeId == SrcClean.question_code_id
        ).filter(
            Code.value.in_([STREET_ADDRESS_QUESTION_CODE, STREET_ADDRESS2_QUESTION_CODE])
        ).all()

        # Make sure we only get the updated address and nothing from the original response
        self.assertEqual(
            1,
            len(address_answers),
            'The updated street address should overwrite the previous answers for line 1 and 2 of the address'
        )
        self.assertEqual(expected_final_address, address_answers[0].value_string)

    def test_in_progress_responses_are_filtered_out_of_export(self):
        """
        We will filter in-progress questionnaire responses out during the ETL process until the curation team is
        ready to start receiving them.
        """

        # Create an in-progress questionnaire response that should not appear in src_clean
        participant = self.data_generator.create_database_participant()
        self._setup_questionnaire_response(
            participant,
            self.questionnaire,
            status=QuestionnaireResponseStatus.IN_PROGRESS
        )
        self.run_cdm_data_generation()

        # Check that src_clean doesn't have any records for the in-progress questionnaire response
        src_clean_answers = self.session.query(SrcClean).filter(
            SrcClean.participant_id == participant.participantId
        ).all()
        self.assertEmpty(src_clean_answers)

    def test_later_in_progress_response_not_used(self):
        """
        Make sure later, in-progress responses don't make us filter out full and valid responses that should be used
        """

        # Create a questionnaire response that might be used instead of the default for the test suite
        in_progress_response = self._setup_questionnaire_response(
            self.participant,
            self.questionnaire,
            authored=datetime(2020, 5, 10),
            created=datetime(2020, 5, 10),
            status=QuestionnaireResponseStatus.IN_PROGRESS
        )
        self.run_cdm_data_generation()

        # Make sure src_clean only has data from the full response
        in_progress_answers = self.session.query(SrcClean).filter(
            SrcClean.questionnaire_response_id == in_progress_response.questionnaireResponseId
        ).all()
        self.assertEmpty(in_progress_answers)

        complete_answers = self.session.query(SrcClean).filter(
            SrcClean.questionnaire_response_id == self.questionnaire_response.questionnaireResponseId
        ).all()
        self.assertNotEmpty(complete_answers)

    def test_duplicate_record_in_temp_questionnaire_response_filtered_out(self):
        """
        Test that duplicate record in cdm.tmp_questionnaire_response is not included in export
        """

        # Create a new questionnaire response
        participant = self.data_generator.create_database_participant()
        self._setup_questionnaire_response(
            participant,
            self.questionnaire
        )

        # Create another response that is a duplicate
        duplicate_response = self._setup_questionnaire_response(
            participant,
            self.questionnaire,
            classification_type=QuestionnaireResponseClassificationType.DUPLICATE
        )

        self.run_cdm_data_generation()

        # Make sure no answers from the duplicate response made it into SrcClean
        src_clean_answers = self.session.query(SrcClean).filter(
            SrcClean.participant_id == participant.participantId
        ).all()
        self.assertFalse(any(
            answer_record.questionnaire_response_id == duplicate_response.questionnaireResponseId
            for answer_record in src_clean_answers
        ))

    def test_zip_code_maps_to_string_field(self):
        """
        There are some questionnaire responses that have the zip code transmitted to us in the valueInteger
        field. Curation is expecting zip codes to be exported to them as strings. This checks to make sure that
        they're mapped correctly.
        """

        # Two codes have used value_integer for transmitting the value
        employment_zipcode_code = self.data_generator.create_database_code(value=EMPLOYMENT_ZIPCODE_QUESTION_CODE)
        address_zipcode_code = self.data_generator.create_database_code(value=ZIPCODE_QUESTION_CODE)
        skip_code = self.data_generator.create_database_code(value=PMI_SKIP_CODE)

        # Create a questionnaire with zip code questions
        zip_code_questionnaire = self.data_generator.create_database_questionnaire_history()
        for index in range(2):  # Creating four questions to test PMI_SKIP and when the value_string is correctly used
            self.data_generator.create_database_questionnaire_question(
                questionnaireId=zip_code_questionnaire.questionnaireId,
                questionnaireVersion=zip_code_questionnaire.version,
                codeId=employment_zipcode_code.codeId
            )
            self.data_generator.create_database_questionnaire_question(
                questionnaireId=zip_code_questionnaire.questionnaireId,
                questionnaireVersion=zip_code_questionnaire.version,
                codeId=address_zipcode_code.codeId
            )
        self.data_generator.create_database_questionnaire_concept(
            questionnaireId=zip_code_questionnaire.questionnaireId,
            questionnaireVersion=self.questionnaire.version,
            codeId=self.module_code.codeId
        )

        # Set up a response with zip code values transmitted in various ways
        expected_zip_code_answers = [
            (0, 'valueInteger', '90210'),
            (1, 'valueInteger', '12345'),
            (2, 'valueString', '12121'),
            (3, 'valueCodeId', skip_code.codeId),
        ]
        zip_code_response = self._setup_questionnaire_response(
            self.participant,
            zip_code_questionnaire,
            indexed_answers=expected_zip_code_answers
        )

        self.run_cdm_data_generation()
        src_clean_answers = self.session.query(SrcClean).filter(
            SrcClean.questionnaire_response_id == zip_code_response.questionnaireResponseId
        ).all()

        for index, answer_field, expected_value in expected_zip_code_answers:
            src_cln: SrcClean = src_clean_answers[index]
            if answer_field == 'valueCodeId':  # An answer of skipping the zip code
                self.assertEqual(expected_value, src_cln.value_code_id)
            else:
                self.assertEqual(expected_value, src_cln.value_string)
                self.assertIsNone(src_cln.value_number)

    def test_exclude_participants_age_under_18(self):
        """
        Curation team request to exclude participants that were under 18 at the time of study consent.
        """
        self._create_consent_questionnaire()
        self.run_cdm_data_generation()
        src_clean_answers = self.session.query(SrcClean).all()
        self.assertEqual(4, len(src_clean_answers))

        dob = datetime(1982, 1, 11)
        self.data_generator.create_database_participant_summary(participant=self.participant, dateOfBirth=dob)
        self.run_cdm_data_generation()
        src_clean_answers = self.session.query(SrcClean).all()
        self.assertEqual(0, len(src_clean_answers))

    def test_etl_exclude_code(self):
        self.run_cdm_data_generation()
        src_clean_answers = self.session.query(SrcClean).all()
        self.assertEqual(4, len(src_clean_answers))
        self.session.commit()

        exclude_question_str = self.question_code_list[0].value + ',' + self.question_code_list[1].value
        self.run_exclude_code_command('add', exclude_question_str, 'question')
        exclude_answer_str = 'a_' + str(self.indexed_answers[2][0])
        self.run_exclude_code_command('add', exclude_answer_str, 'answer')

        self.run_cdm_data_generation()
        src_clean_answers = self.session.query(SrcClean).all()
        self.assertEqual(1, len(src_clean_answers))
        self.session.commit()

        self.run_exclude_code_command('add', self.module_code.value, 'module')
        self.run_cdm_data_generation()
        src_clean_answers = self.session.query(SrcClean).all()
        self.assertEqual(0, len(src_clean_answers))

        self.clear_table_after_test('cdr_etl_survey_history')
        self.clear_table_after_test('cdr_etl_run_history')
        self.clear_table_after_test('cdr_excluded_code')

    def test_etl_include_exclude_survey_history(self):
        exclude_question_str = self.question_code_list[0].value + ',' + self.question_code_list[1].value
        self.run_exclude_code_command('add', exclude_question_str, 'question')
        exclude_answer_str = 'a_' + str(self.indexed_answers[2][0])
        self.run_exclude_code_command('add', exclude_answer_str, 'answer')

        self.run_cdm_data_generation()
        src_clean_answers = self.session.query(SrcClean).all()
        self.assertEqual(1, len(src_clean_answers))
        self.session.commit()

        include_exclude_history_dao = CdrEtlSurveyHistoryDao()
        records = include_exclude_history_dao.get_all()
        self.assertEqual(len(records), 6)

        self.clear_table_after_test('cdr_etl_survey_history')
        self.clear_table_after_test('cdr_etl_run_history')
        self.clear_table_after_test('cdr_excluded_code')

    def test_etl_history(self):
        with FakeClock(TIME_2):
            self.run_cdm_data_generation(cutoff='2022-04-01')
        dao = CdrEtlRunHistoryDao()
        records = dao.get_all()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].cutoffDate, date(2022, 4, 1))
        self.assertEqual(records[0].startTime, TIME_2)
        self.assertEqual(records[0].endTime, TIME_2)
        self.assertEqual(records[0].vocabularyPath, 'gs://curation-vocabulary/aou_vocab_20220201/')

        self.clear_table_after_test('cdr_etl_survey_history')
        self.clear_table_after_test('cdr_etl_run_history')
        self.clear_table_after_test('cdr_excluded_code')

    def test_cutoff_date(self):
        participant1 = self.data_generator.create_database_participant()
        # consent before cutoff 2022-04-01, should be included in the result
        self.data_generator.create_database_participant_summary(
            participant=participant1, consentForStudyEnrollmentFirstYesAuthored=datetime(2022, 2, 1),
            dateOfBirth=datetime(1982, 1, 9))
        self._setup_questionnaire_response(
            participant1,
            self.questionnaire,
            authored=datetime(2022, 2, 1)
        )

        participant2 = self.data_generator.create_database_participant()
        # consent after cutoff 2022-04-01, should not be included in the result
        self.data_generator.create_database_participant_summary(
            participant=participant2, consentForStudyEnrollmentFirstYesAuthored=datetime(2022, 4, 20),
            dateOfBirth=datetime(1982, 1, 9))
        self._setup_questionnaire_response(
            participant2,
            self.questionnaire,
            authored=datetime(2022, 4, 20)
        )

        participant3 = self.data_generator.create_database_participant()
        # withdrawal before cutoff 2022-04-01, should not be included in the result
        self.data_generator.create_database_participant_summary(
            participant=participant3, consentForStudyEnrollmentFirstYesAuthored=datetime(2022, 2, 1),
            withdrawalStatus=2, withdrawalAuthored=datetime(2022, 2, 20), dateOfBirth=datetime(1982, 1, 9))
        self._setup_questionnaire_response(
            participant3,
            self.questionnaire,
            authored=datetime(2022, 2, 1)
        )

        participant4 = self.data_generator.create_database_participant()
        # withdrawal after cutoff 2022-04-01, should be included in the result
        self.data_generator.create_database_participant_summary(
            participant=participant4, consentForStudyEnrollmentFirstYesAuthored=datetime(2022, 2, 1),
            withdrawalStatus=2, withdrawalAuthored=datetime(2022, 4, 20), dateOfBirth=datetime(1982, 1, 9))
        self._setup_questionnaire_response(
            participant4,
            self.questionnaire,
            authored=datetime(2022, 2, 1)
        )

        self.run_cdm_data_generation(cutoff='2022-04-01')

        src_clean_answers_p1 = self.session.query(SrcClean)\
            .filter(SrcClean.participant_id == participant1.participantId).all()
        self.assertEqual(4, len(src_clean_answers_p1))

        src_clean_answers_p2 = self.session.query(SrcClean) \
            .filter(SrcClean.participant_id == participant2.participantId).all()
        self.assertEqual(0, len(src_clean_answers_p2))

        src_clean_answers_p3 = self.session.query(SrcClean) \
            .filter(SrcClean.participant_id == participant3.participantId).all()
        self.assertEqual(0, len(src_clean_answers_p3))

        src_clean_answers_p4 = self.session.query(SrcClean) \
            .filter(SrcClean.participant_id == participant4.participantId).all()
        self.assertEqual(4, len(src_clean_answers_p4))

    def test_ignored_answers_are_marked_invalid(self):
        """
        Any answers that have the ignore field set to True should give the skip code
        (to be marked invalid in the finalization step)
        """
        questionnaire_response = self._setup_questionnaire_response(
            self.participant,
            self.questionnaire,
            indexed_answers=[
                (0, 'valueString', 'first_answer'),
                (1, 'valueString', 'ignored answer'),
                (2, 'valueBoolean', True),
                (3, 'valueDate', datetime.now())
            ],
            ignored_answer_indexes=[1, 2, 3]
        )

        # Check that we are only be seeing the answers from the latest questionnaire response
        self.run_cdm_data_generation()
        src_clean_answers: Collection[SrcClean] = self.session.query(SrcClean).filter(
            SrcClean.questionnaire_response_id == questionnaire_response.questionnaireResponseId
        ).all()

        # Should be getting 4 records still (even though some of them are blank)
        self.assertEqual(4, len(src_clean_answers))
        for index, answer in enumerate(src_clean_answers):
            if index == 0:
                self.assertEqual('first_answer', answer.value_string)
            else:
                # Make sure none of the answer fields have an answer value
                self.assertFalse(any([
                    answer is not None
                    for answer in [
                        answer.value_string,
                        answer.value_number,
                        answer.value_date,
                        answer.value_boolean,
                        answer.value_code_id
                    ]
                ]))
                self.assertEqual(PMI_SKIP_CODE, answer.value_ppi_code)

    def test_cutoff_date_questionnaire_response_used(self):
        """The latest questionnaire response received before the cutoff date for a module should be used"""
        # Note: this only applies to modules that shouldn't roll up answers (ConsentPII should be rolled up)

        # Create a questionnaire response that would be used instead of the default for the test suite
        self._setup_questionnaire_response(
            self.participant,
            self.questionnaire,
            indexed_answers=[
                (1, 'valueString', 'update'),
                (3, 'valueString', 'intermediate answer')
            ],
            authored=datetime(2020, 5, 10),
            created=datetime(2020, 5, 10)
        )

        self._setup_questionnaire_response(
            self.participant,
            self.questionnaire,
            indexed_answers=[
                (1, 'valueString', 'update2'),
                (3, 'valueString', 'final answer')
            ],
            authored=datetime(2022, 5, 10),
            created=datetime(2022, 5, 10)
        )

        # Check that we are only seeing the answers from the questionnaire response before cutoff date
        self.run_cdm_data_generation(cutoff='2022-04-01')
        for question_index, question in enumerate(self.questionnaire.questions):
            expected_answer = None
            if question_index == 1:
                expected_answer = 'update'
            elif question_index == 3:
                expected_answer = 'intermediate answer'

            src_clean_answer = self.session.query(SrcClean).filter(
                SrcClean.question_code_id == question.codeId
            ).one_or_none()
            if expected_answer is None:
                self.assertIsNone(src_clean_answer)
            else:
                self.assertEqual(expected_answer, src_clean_answer.value_string)
        self.session.commit()

        # Returns the newest answers without cutoff
        self.run_cdm_data_generation()
        for question_index, question in enumerate(self.questionnaire.questions):
            expected_answer = None
            if question_index == 1:
                expected_answer = 'update2'
            elif question_index == 3:
                expected_answer = 'final answer'

            src_clean_answer = self.session.query(SrcClean).filter(
                SrcClean.question_code_id == question.codeId
            ).one_or_none()
            if expected_answer is None:
                self.assertIsNone(src_clean_answer)
            else:
                self.assertEqual(expected_answer, src_clean_answer.value_string)
