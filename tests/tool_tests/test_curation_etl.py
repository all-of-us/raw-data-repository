from datetime import datetime, date
from typing import Collection, Any
from decimal import Decimal

from rdr_service import clock
from rdr_service.code_constants import CONSENT_FOR_STUDY_ENROLLMENT_MODULE, EMPLOYMENT_ZIPCODE_QUESTION_CODE, \
    PMI_SKIP_CODE, \
    STREET_ADDRESS_QUESTION_CODE, STREET_ADDRESS2_QUESTION_CODE, ZIPCODE_QUESTION_CODE, DATE_OF_BIRTH_QUESTION_CODE, \
    CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_MODULE, EHR_CONSENT_QUESTION_CODE
from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.etl.model.src_clean import SrcClean, Observation, PidRidMapping, Person, Measurement, Death, \
    EHRConsentStatus
from rdr_service.model.code import Code
from rdr_service.model.consent_file import ConsentFile
from rdr_service.model.consent_response import ConsentResponse
from rdr_service.model.measurements import PhysicalMeasurements
from rdr_service.model.measurements import Measurement as RdrMeasurement
from rdr_service.model.participant import Participant
from rdr_service.model.deceased_report import DeceasedReport
from rdr_service.model.api_user import ApiUser
from rdr_service.dao.physical_measurements_dao import PhysicalMeasurementsDao
from rdr_service.dao.curation_etl_dao import CdrEtlRunHistoryDao, CdrEtlSurveyHistoryDao
from rdr_service.participant_enums import QuestionnaireResponseStatus, QuestionnaireResponseClassificationType, \
    PhysicalMeasurementsCollectType, OriginMeasurementUnit, DeceasedNotification, DeceasedReportStatus
from rdr_service.tools.tool_libs.curation import CurationExportClass
from tests.helpers.unittest_base import BaseTestCase
from tests.helpers.tool_test_mixin import ToolTestMixin
from tests import test_data
from rdr_service.clock import FakeClock

TIME = datetime(2000, 1, 10)
TIME_2 = datetime(2022, 5, 10)


class CurationEtlTest(ToolTestMixin, BaseTestCase):
    def setUp(self):
        super(CurationEtlTest, self).setUp(with_consent_codes=True)
        self._setup_data()
        self.history_dao = CdrEtlRunHistoryDao()
        self.pm_dao = PhysicalMeasurementsDao()

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

    def _exists_in_src_clean(self, column_name:str, value:Any) -> bool:
        value_exists = self.session.query(
            getattr(SrcClean,column_name)
        ).filter(
            getattr(SrcClean,column_name) == value
        ).distinct().scalar()
        return bool(value_exists
                    )

    @staticmethod
    def run_cdm_data_generation(cutoff=None, vocabulary='gs://curation-vocabulary/aou_vocab_20220201/',
                                participant_origin='all', participant_list_file=None, include_surveys=None,
                                exclude_surveys=None, exclude_participants=None, omit_surveys=False,
                                omit_measurements=False, exclude_in_person_pm=False, exclude_remote_pm=False):
        CurationEtlTest.run_tool(CurationExportClass, tool_args={
            'command': 'cdm-data',
            'cutoff': cutoff,
            'vocabulary': vocabulary,
            'participant_origin': participant_origin,
            'participant_list_file': participant_list_file,
            'include_surveys': include_surveys,
            'exclude_surveys': exclude_surveys,
            'exclude_participants': exclude_participants,
            'omit_surveys': omit_surveys,
            'omit_measurements': omit_measurements,
            "exclude_in_person_pm": exclude_in_person_pm,
            "exclude_remote_pm": exclude_remote_pm
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
            STREET_ADDRESS_QUESTION_CODE, STREET_ADDRESS2_QUESTION_CODE, DATE_OF_BIRTH_QUESTION_CODE
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
        records = self.history_dao.get_all()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].cutoffDate, date(2022, 4, 1))
        self.assertEqual(records[0].startTime, TIME_2)
        self.assertEqual(records[0].endTime, TIME_2)
        self.assertEqual(records[0].vocabularyPath, 'gs://curation-vocabulary/aou_vocab_20220201/')
        self.assertEqual(records[0].filterOptions['participant_origin'],'all')

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

    def test_exclude_profile_update_questionnaire_response(self):
        """Questionnaire Responses with PROFILE_UPDATE classification should be excluded"""


        # Create a questionnaire response that would be used instead of the default for the test suite
        self._setup_questionnaire_response(
            self.participant,
            self.questionnaire,
            indexed_answers=[
                (1, 'valueString', 'update'),
                (3, 'valueString', 'final answer')
            ],
            authored=datetime(2020, 6, 10),
            created=datetime(2020, 6, 10)
        )

        self._setup_questionnaire_response(
            self.participant,
            self.questionnaire,
            indexed_answers=[
                (1, 'valueString', 'profile update'),
            ],
            authored=datetime(2021, 6, 10),
            created=datetime(2021, 6, 10),
            classification_type=QuestionnaireResponseClassificationType.PROFILE_UPDATE
        )

        # Check that we are only seeing the answers from the latest questionnaire response
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

    def test_participant_origin_filter(self):
        """ Test the participant origin filter selects the intended participants"""
        participant_ce = self.data_generator.create_database_participant(participantOrigin='careevolution')
        self.data_generator.create_database_participant_summary(
            participant=participant_ce,
            dateOfBirth=datetime(1982, 1, 9),
            consentForStudyEnrollmentFirstYesAuthored=datetime(2000, 1, 10))
        self._setup_questionnaire_response(
            participant_ce,
            self.questionnaire
        )

        participant_vibrent = self.data_generator.create_database_participant(participantOrigin='vibrent')
        self.data_generator.create_database_participant_summary(
            participant=participant_vibrent,
            dateOfBirth=datetime(1982, 1, 9),
            consentForStudyEnrollmentFirstYesAuthored=datetime(2000, 1, 10))
        self._setup_questionnaire_response(
            participant_vibrent,
            self.questionnaire
        )

        self.run_cdm_data_generation(participant_origin='careevolution')
        vibrent_ppt_exists = self._exists_in_src_clean('participant_id', participant_vibrent.participantId)
        ce_ppt_exists = self._exists_in_src_clean('participant_id', participant_ce.participantId)

        self.assertTrue(ce_ppt_exists)
        self.assertFalse(vibrent_ppt_exists)

        run_history = self.history_dao.get_last_etl_run_info(self.session)
        self.assertEqual('careevolution', run_history.filterOptions['participant_origin'])

        self.session.commit()

        self.run_cdm_data_generation(participant_origin='vibrent')
        vibrent_ppt_exists = self._exists_in_src_clean('participant_id', participant_vibrent.participantId)
        ce_ppt_exists = self._exists_in_src_clean('participant_id', participant_ce.participantId)

        self.assertFalse(ce_ppt_exists)
        self.assertTrue(vibrent_ppt_exists)

        run_history = self.history_dao.get_last_etl_run_info(self.session)
        self.assertEqual('vibrent', run_history.filterOptions['participant_origin'])

        self.session.commit()

        self.run_cdm_data_generation()
        vibrent_ppt_exists = self._exists_in_src_clean('participant_id', participant_vibrent.participantId)
        ce_ppt_exists = self._exists_in_src_clean('participant_id', participant_ce.participantId)

        self.assertTrue(ce_ppt_exists)
        self.assertTrue(vibrent_ppt_exists)

        run_history = self.history_dao.get_last_etl_run_info(self.session)
        self.assertEqual('all', run_history.filterOptions['participant_origin'])

    def test_participant_list(self):
        pids = list(range(10000,10010))
        for pid in pids:
            participant = self.data_generator.create_database_participant(participantId=pid)
            self.data_generator.create_database_participant_summary(
                participant=participant,
                dateOfBirth=datetime(1982, 1, 9),
                consentForStudyEnrollmentFirstYesAuthored=datetime(2000, 1, 10))
            self._setup_questionnaire_response(
                participant,
                self.questionnaire
            )

        self.run_cdm_data_generation(
            participant_origin=None,
            participant_list_file=test_data.data_path('test_curation_participant_list.txt')
        )

        test_participants = [10002, 10004, 10007]
        for pid in pids:
            pid_exists = self._exists_in_src_clean("participant_id", pid)
            if pid in test_participants:
                self.assertTrue(pid_exists)
            else:
                self.assertFalse(pid_exists)

        run_history = self.history_dao.get_last_etl_run_info(self.session)
        self.assertIn('test-data/test_curation_participant_list.txt',
                      run_history.filterOptions['participant_list_file'])

    def _create_questionnaire(self, survey_name, question_code_list=None):
        module_code = self.data_generator.create_database_code(value=survey_name)
        question_codes = [
            self.data_generator.create_database_code(value=f'{survey_name}_q_code_{question_index}')
            for question_index in range(4)
        ]

        if question_code_list:
            question_codes += self.session.query(Code).filter(Code.value.in_([
                question_code_list
            ])).all()

        questionnaire = self.data_generator.create_database_questionnaire_history()
        for question_code in question_codes:
            self.data_generator.create_database_questionnaire_question(
                questionnaireId=questionnaire.questionnaireId,
                questionnaireVersion=questionnaire.version,
                codeId=question_code.codeId
            )

        self.data_generator.create_database_questionnaire_concept(
            questionnaireId=questionnaire.questionnaireId,
            questionnaireVersion=questionnaire.version,
            codeId=module_code.codeId
        )

        return questionnaire

    def test_include_surveys(self):
        survey_names = ['Q1', 'Q2', 'Q3', 'Q4']
        for survey_name in survey_names:
            questionnaire = self._create_questionnaire(survey_name)
            self._setup_questionnaire_response(self.participant, questionnaire)

        self.run_cdm_data_generation(include_surveys=test_data.data_path('test_curation_surveys.txt'))

        survey_results = []
        for survey_name in survey_names:
            survey_results.append((survey_name, self._exists_in_src_clean('survey_name', survey_name)))

        for result in survey_results:
            if result[0] in ['Q1', 'Q4']:
                self.assertFalse(result[1])
            elif result[0] in ['Q2', 'Q3']:
                self.assertTrue(result[1])

        last_run = self.history_dao.get_last_etl_run_info(self.session)
        self.assertEqual(['Q2', 'Q3'], last_run.filterOptions['include_surveys'])

    def test_exclude_surveys(self):
        survey_names = ['Q1', 'Q2', 'Q3', 'Q4']
        for survey_name in survey_names:
            questionnaire = self._create_questionnaire(survey_name)
            self._setup_questionnaire_response(self.participant, questionnaire)

        self.run_cdm_data_generation(exclude_surveys=test_data.data_path('test_curation_surveys.txt'))

        survey_results = []
        for survey_name in survey_names:
            survey_results.append((survey_name, self._exists_in_src_clean('survey_name', survey_name)))

        for result in survey_results:
            if result[0] in ['Q1', 'Q4', 'src_clean_test']:
                self.assertTrue(result[1], f"{result[0]} expected in src_clean but not found")
            elif result[0] in ['Q2', 'Q3']:
                self.assertFalse(result[1])

        last_run = self.history_dao.get_last_etl_run_info(self.session)
        self.assertEqual(['Q2', 'Q3'], last_run.filterOptions['exclude_surveys'])

    def test_exclude_participants(self):
        pids = list(range(10000,10010))
        for pid in pids:
            participant = self.data_generator.create_database_participant(participantId=pid)
            self.data_generator.create_database_participant_summary(
                participant=participant,
                dateOfBirth=datetime(1982, 1, 9),
                consentForStudyEnrollmentFirstYesAuthored=datetime(2000, 1, 10))
            self._setup_questionnaire_response(
                participant,
                self.questionnaire
            )

        self.run_cdm_data_generation(
            participant_origin='all',
            exclude_participants=test_data.data_path('test_curation_participant_list.txt')
        )

        excluded_participants = [10002, 10004, 10007]
        for pid in pids:
            pid_exists = self._exists_in_src_clean("participant_id", pid)
            if pid in excluded_participants:
                self.assertFalse(pid_exists)
            else:
                self.assertTrue(pid_exists)

        run_history = self.history_dao.get_last_etl_run_info(self.session)
        self.assertIn('test-data/test_curation_participant_list.txt',
                         run_history.filterOptions['participant_exclude_file'])

    def test_survey_src_id(self):
        consent_questionnaire = self._create_consent_questionnaire()

        participant = self.data_generator.create_database_participant(participantOrigin='test_portal')
        self.data_generator.create_database_participant_summary(
            participant=participant,
            dateOfBirth=datetime(1982, 1, 9),
            consentForStudyEnrollmentFirstYesAuthored=datetime(2000, 1, 10)
        )
        self._setup_questionnaire_response(
            participant,
            self.questionnaire
        )
        self._setup_questionnaire_response(
            participant,
            consent_questionnaire,
            indexed_answers=[
                (6, 'valueDate', datetime(1982, 1, 9))
                # Assuming the 6th question is the date of birth
            ],
            authored=datetime(2020, 5, 1)
        )

        self.run_cdm_data_generation(
            participant_origin='all',
        )

        obs_src_id = self.session.query(
            Observation.src_id
        ).filter(
            Observation.person_id == participant.participantId
        ).first()[0]

        prm_src_id = self.session.query(
            PidRidMapping.src_id
        ).filter(
            PidRidMapping.person_id == participant.participantId
        ).first()[0]

        person_src_id = self.session.query(
            Person.src_id
        ).filter(
            Person.person_id == participant.participantId
        ).first()[0]

        self.assertEqual('test_portal', obs_src_id)
        self.assertEqual('test_portal', prm_src_id)
        self.assertEqual('test_portal', person_src_id)

    def _setup_pm(self, participant_id: int):
        """ Creates in-person and remote physical measurements for a participant_id"""
        resource = {"entry": [
            {"resource":
                 {"date": datetime.now().isoformat()}
             }
        ]}

        in_person_pm_data = {
            "physicalMeasurementsId": 1,
            "participantId": participant_id,
            "createdSiteId": 1,
            "finalizedSiteId": 2,
            "origin": 'hpro',
            "collectType": PhysicalMeasurementsCollectType.SITE,
            "originMeasurementUnit": OriginMeasurementUnit.UNSET
        }

        record: PhysicalMeasurements = PhysicalMeasurements(**in_person_pm_data)
        self.pm_dao.store_record_fhir_doc(record, resource)
        pm_record: PhysicalMeasurements = self.pm_dao.insert(record)

        meas_in_person_height = RdrMeasurement(
            measurementId=10001,
            physicalMeasurementsId=pm_record.physicalMeasurementsId,
            codeSystem="http://terminology.pmi-ops.org/CodeSystem/physical-measurements",
            codeValue="height",
            measurementTime=datetime.now(),
            valueDecimal=162.0,
            valueUnit="cm",
        )
        meas_in_person_weight = RdrMeasurement(
            measurementId=10002,
            physicalMeasurementsId=pm_record.physicalMeasurementsId,
            codeSystem="http://terminology.pmi-ops.org/CodeSystem/physical-measurements",
            codeValue="weight",
            measurementTime=datetime.now(),
            valueDecimal=63.0,
            valueUnit="kg",
        )
        self.session.add(meas_in_person_weight)
        self.session.add(meas_in_person_height)

        remote_pm_data = {
            "physicalMeasurementsId": 2,
            "participantId": participant_id,
            "createdSiteId": 1,
            "finalizedSiteId": 2,
            "origin": 'test-portal',
            "collectType": PhysicalMeasurementsCollectType.SELF_REPORTED,
            "originMeasurementUnit": OriginMeasurementUnit.UNSET
        }

        record: PhysicalMeasurements = PhysicalMeasurements(**remote_pm_data)
        self.pm_dao.store_record_fhir_doc(record, resource)
        pm_record: PhysicalMeasurements = self.pm_dao.insert(record)

        meas_remote_height = RdrMeasurement(
            measurementId=10005,
            physicalMeasurementsId=pm_record.physicalMeasurementsId,
            codeSystem="http://terminology.pmi-ops.org/CodeSystem/physical-measurements",
            codeValue="height",
            measurementTime=datetime.now(),
            valueDecimal=165.0,
            valueUnit="cm",
        )
        meas_remote_weight = RdrMeasurement(
            measurementId=10006,
            physicalMeasurementsId=pm_record.physicalMeasurementsId,
            codeSystem="http://terminology.pmi-ops.org/CodeSystem/physical-measurements",
            codeValue="weight",
            measurementTime=datetime.now(),
            valueDecimal=62.0,
            valueUnit="kg",
        )

        self.session.add(meas_remote_weight)
        self.session.add(meas_remote_height)
        self.session.commit()

    def test_pm_src_id(self):
        consent_questionnaire = self._create_consent_questionnaire()
        participant = self.data_generator.create_database_participant(participantOrigin='test_portal')
        self.data_generator.create_database_participant_summary(
            participant=participant,
            dateOfBirth=datetime(1982, 1, 9),
            consentForStudyEnrollmentFirstYesAuthored=datetime(2000, 1, 10)
        )
        self._setup_questionnaire_response(
            participant,
            consent_questionnaire,
            indexed_answers=[
                (6, 'valueDate', datetime(1982, 1, 9))
                # Assuming the 6th question is the date of birth
            ],
            authored=datetime(2020, 5, 1)
        )

        self._setup_pm(participant.participantId)

        self.run_cdm_data_generation(
            participant_origin='all',
        )

        inperson_src_id = self.session.query(
            Measurement.src_id
        ).filter(
            Measurement.person_id == participant.participantId,
            Measurement.measurement_type_concept_id == 44818701
        ).first()[0]

        remote_src_id = self.session.query(
            Measurement.src_id
        ).filter(
            Measurement.person_id == participant.participantId,
            Measurement.measurement_type_concept_id == 32865
        ).first()[0]

        self.assertEqual('hpro', inperson_src_id)
        self.assertEqual('test-portal', remote_src_id)

    def test_include_physical_measurements(self):
        consent_questionnaire = self._create_consent_questionnaire()
        participant = self.data_generator.create_database_participant(participantOrigin='test_portal')
        self.data_generator.create_database_participant_summary(
            participant=participant,
            dateOfBirth=datetime(1982, 1, 9),
            consentForStudyEnrollmentFirstYesAuthored=datetime(2000, 1, 10)
        )
        self._setup_questionnaire_response(
            participant,
            consent_questionnaire,
            indexed_answers=[
                (6, 'valueDate', datetime(1982, 1, 9))
                # Assuming the 6th question is the date of birth
            ],
            authored=datetime(2020, 5, 1)
        )
        self._setup_pm(participant.participantId)
        in_person_height = (44818701, 'height', Decimal('162.000000'), 'cm')
        in_person_weight = (44818701, 'weight', Decimal('63.000000'), 'kg')
        remote_height = (32865, 'height', Decimal('165.000000'), 'cm')
        remote_weight = (32865, 'weight', Decimal('62.000000'), 'kg')

        self.run_cdm_data_generation(
            participant_origin='all',
            exclude_in_person_pm=False,
            exclude_remote_pm=False
        )

        measurements = self.session.query(Measurement.measurement_type_concept_id,
                                          Measurement.measurement_source_value,
                                          Measurement.value_as_number,
                                          Measurement.unit_source_value).all()

        # Check if in-person and remote measurements are in table
        self.assertIn(in_person_height, measurements)
        self.assertIn(in_person_weight, measurements)
        self.assertIn(remote_height, measurements)
        self.assertIn(remote_weight, measurements)

        self.session.commit()

        self.run_cdm_data_generation(
            participant_origin='all',
            exclude_in_person_pm=True,
            exclude_remote_pm=False
        )

        measurements = self.session.query(Measurement.measurement_type_concept_id,
                                          Measurement.measurement_source_value,
                                          Measurement.value_as_number,
                                          Measurement.unit_source_value).all()

        # Only remote measurements should be present
        self.assertNotIn(in_person_height, measurements)
        self.assertNotIn(in_person_weight, measurements)
        self.assertIn(remote_height, measurements)
        self.assertIn(remote_weight, measurements)

        self.session.commit()

        self.run_cdm_data_generation(
            participant_origin='all',
            exclude_in_person_pm=False,
            exclude_remote_pm=True
        )

        measurements = self.session.query(Measurement.measurement_type_concept_id,
                                          Measurement.measurement_source_value,
                                          Measurement.value_as_number,
                                          Measurement.unit_source_value).all()

        # Only in-person measurements should be present
        self.assertIn(in_person_height, measurements)
        self.assertIn(in_person_weight, measurements)
        self.assertNotIn(remote_height, measurements)
        self.assertNotIn(remote_weight, measurements)

    def test_death_table(self):
        # Create API User for DeceasedReport
        api_user = ApiUser(id=1, username='test', system='test')
        self.session.add(api_user)

        pids = list(range(10000, 10010))
        consent_questionnaire = self._create_consent_questionnaire()
        for pid in pids:
            participant = self.data_generator.create_database_participant(participantId=pid)
            self.data_generator.create_database_participant_summary(
                participant=participant,
                dateOfBirth=datetime(1982, 1, 9),
                consentForStudyEnrollmentFirstYesAuthored=datetime(2000, 1, 10)
            )
            self._setup_questionnaire_response(
                participant,
                consent_questionnaire,
                indexed_answers=[
                    (6, 'valueDate', datetime(1982, 1, 9))
                    # Assuming the 6th question is the date of birth
                ],
                authored=datetime(2020, 5, 1)
            )

        # Only approved deceased reports should be in Death
        deceased_reports = [
            DeceasedReport(
                participantId=pids[0],
                dateOfDeath=date(2023, 2, 1),
                status=DeceasedReportStatus.APPROVED,
                notification=DeceasedNotification.EHR,
                authorId=1,
                authored=datetime(2023, 3, 1)
            ),
            DeceasedReport(
                participantId=pids[1],
                dateOfDeath=date(2023, 2, 1),
                status=DeceasedReportStatus.PENDING,
                notification=DeceasedNotification.EHR,
                authorId=1,
                authored=datetime(2023, 3, 1)
            ),
            DeceasedReport(
                participantId=pids[2],
                dateOfDeath=date(2023, 2, 1),
                status=DeceasedReportStatus.DENIED,
                notification=DeceasedNotification.EHR,
                authorId=1,
                authored=datetime(2023, 3, 1)
            )
        ]

        for deceased_report in deceased_reports:
            self.session.add(deceased_report)
        self.session.commit()

        self.run_cdm_data_generation(
            participant_origin='all',
        )

        def _exists_in_death_table(participant_id: int) -> bool:
            return bool(self.session.query(
                Death.person_id
            ).filter(
                Death.person_id == participant_id
            ).distinct().scalar())

        self.assertTrue(_exists_in_death_table(pids[0]))
        self.assertFalse(_exists_in_death_table(pids[1]))
        self.assertFalse(_exists_in_death_table(pids[2]))

    def test_ehr_consent_table(self):
        consent_dao = ConsentDao()
        self.data_generator.create_database_code(value=EHR_CONSENT_QUESTION_CODE)
        ehr_consent = self._create_questionnaire(CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_MODULE,
                                                        [EHR_CONSENT_QUESTION_CODE])
        ehr_yes = self.data_generator.create_database_code(value="Yes")
        ehr_no = self.data_generator.create_database_code(value="No")

        questionnaire_response_ids = []
        submission_date = datetime(2023, 4, 29, 0, 0)

        for pid in range(10000,10006):
            participant = self.data_generator.create_database_participant(participantId=pid,
                                                                          researchId=pid+100)
            self.data_generator.create_database_participant_summary(
                participant=participant,
                dateOfBirth=datetime(1982, 1, 9),
                consentForStudyEnrollmentFirstYesAuthored=datetime(2000, 1, 10)
            )
            if pid == 10000:
                self._setup_questionnaire_response(
                    participant,
                    ehr_consent,
                    indexed_answers=[
                        (4, 'valueCodeId', ehr_no.codeId)
                    ],
                    authored=submission_date
                )
            else:
                qr = self._setup_questionnaire_response(
                    participant,
                    ehr_consent,
                    indexed_answers=[
                        (4, 'valueCodeId', ehr_yes.codeId)
                    ],
                    authored=submission_date
                )
                questionnaire_response_ids.append(qr.questionnaireResponseId)


        consent_responses = [
            ConsentResponse(
                id=2,
                questionnaire_response_id=questionnaire_response_ids[1], # PID 10002
            ),
            ConsentResponse(
                id=3,
                questionnaire_response_id=questionnaire_response_ids[2], # PID 10003
            ),
            ConsentResponse(
                id=4,
                questionnaire_response_id=questionnaire_response_ids[3], # PID 10004
            ),
            ConsentResponse(
                id=5,
                questionnaire_response_id=questionnaire_response_ids[4], # PID 10005
            )
        ]
        for consent_response in consent_responses:
            self.session.add(consent_response)
        self.session.commit()

        with clock.FakeClock(datetime(2023, 5, 2)):
            consent_dao.insert(ConsentFile(
                    consent_response_id=2, # PID 10002
                    sync_status=2
                ))

        with clock.FakeClock(datetime(2023, 4, 30)):
            consent_files = [
                ConsentFile(
                    consent_response_id=3, # PID 10003
                    sync_status=2
                ),
                ConsentFile(
                    consent_response_id=4, # PID 10004
                    sync_status=1
                ),
            ]
            for consent_file in consent_files:
                consent_dao.insert(consent_file)


        self.run_cdm_data_generation(
            participant_origin='all',
            cutoff='2023-05-01'
        )

        expected_results = [
            (10000, 10100, 'SUBMITTED_NO', submission_date), # Submitted No answer
            (10001, 10101, 'SUBMITTED', submission_date), # Submitted Yes before validation implemented
            (10002, 10102, 'SUBMITTED_NOT_VALIDATED', submission_date), # Submitted Yes, not validated before cutoff date
            (10003, 10103, 'SUBMITTED', submission_date), # Submitted Yes, valid consent
            (10004, 10104, 'SUBMITTED_INVALID', submission_date), # Submitted Yes, invalid consent
            (10005, 10105, 'SUBMITTED_NOT_VALIDATED', submission_date) # Submitted yes, Consent Response exists, Consent file does not, not yet validated
        ]

        consent_table = self.session.query(
            EHRConsentStatus.person_id,
            EHRConsentStatus.research_id,
            EHRConsentStatus.consent_for_electronic_health_records,
            EHRConsentStatus.consent_for_electronic_health_records_authored
        ).all()

        for result in expected_results:
            self.assertIn(result, consent_table)
