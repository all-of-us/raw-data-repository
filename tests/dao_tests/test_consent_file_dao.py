from datetime import datetime
from typing import List

from rdr_service.code_constants import PRIMARY_CONSENT_UPDATE_QUESTION_CODE
from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.model.consent_file import ConsentSyncStatus, ConsentType
from rdr_service.model.consent_response import ConsentResponse
from rdr_service.participant_enums import QuestionnaireStatus
from tests.helpers.unittest_base import BaseTestCase


class ConsentFileDaoTest(BaseTestCase):
    def setUp(self, *args, **kwargs) -> None:
        super(ConsentFileDaoTest, self).setUp(*args, **kwargs)
        self.consent_dao = ConsentDao()

        # Initialize questionnaire for primary update
        self.primary_update_questionnaire = self.data_generator.create_database_questionnaire_history()
        primary_update_question_code = self.data_generator.create_database_code(
            value=PRIMARY_CONSENT_UPDATE_QUESTION_CODE
        )
        self.primary_update_question = self.data_generator.create_database_questionnaire_question(
            codeId=primary_update_question_code.codeId,
            questionnaireId=self.primary_update_questionnaire.questionnaireId,
            questionnaireVersion=self.primary_update_questionnaire.version
        )

    def test_finding_validations_needed_by_response(self):
        # Set up a pair of QuestionnaireResponses, one of which needs to be validated
        response_to_validate = self.data_generator.create_database_questionnaire_response()
        self.session.add(ConsentResponse(response=response_to_validate, type=ConsentType.EHR))

        ignored_response = self.data_generator.create_database_questionnaire_response()
        consent_response = ConsentResponse(response=ignored_response, type=ConsentType.PRIMARY)
        self.data_generator.create_database_consent_file(
            consent_response=consent_response,
            participant_id=ignored_response.participantId,
            type=ConsentType.PRIMARY
        )

        self.session.commit()

        # Make sure we get the correct response from the DAO
        pid_consent_response_map, _ = self.consent_dao.get_consent_responses_to_validate(session=self.session)
        self.assertNotIn(ignored_response.participantId, pid_consent_response_map)

        consent_response = pid_consent_response_map[response_to_validate.participantId][0]
        self.assertEqual(response_to_validate.questionnaireResponseId, consent_response.questionnaire_response_id)

    def test_getting_recent_response(self):
        filter_date = datetime(2022, 4, 1)

        # add a few before the filter date
        self.session.add(
            ConsentResponse(response=self.data_generator.create_database_questionnaire_response(
                created=datetime(2021, 11, 8)
            ))
        )
        self.session.add(
            ConsentResponse(response=self.data_generator.create_database_questionnaire_response(
                created=datetime(2022, 3, 25)
            ))
        )

        # add one after the filter date
        response_to_validate = self.data_generator.create_database_questionnaire_response(
            created=datetime(2022, 4, 5)
        )
        self.session.add(ConsentResponse(response=response_to_validate, type=ConsentType.EHR))
        self.session.commit()

        # Make sure we get the correct response from the DAO
        pid_consent_response_map, _ = self.consent_dao.get_consent_responses_to_validate(session=self.session)

        consent_response = pid_consent_response_map[response_to_validate.participantId][0]
        self.assertEqual(response_to_validate.questionnaireResponseId, consent_response.questionnaire_response_id)

    def test_finding_consent_responses_by_participant(self):
        # create a few consent responses for a participant
        participant = self.data_generator.create_database_participant()
        primary_response = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            authored=datetime(2020, 1, 7)
        )
        reconsent = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            authored=datetime(2021, 10, 2)
        )
        ehr_response = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            authored=datetime(2020, 4, 26)
        )
        self.session.add(ConsentResponse(response=primary_response, type=ConsentType.PRIMARY))
        self.session.add(ConsentResponse(response=reconsent, type=ConsentType.PRIMARY))
        self.session.add(ConsentResponse(response=ehr_response, type=ConsentType.EHR))
        self.session.commit()

        consent_responses = self.consent_dao.get_consent_authored_times_for_participant(
            participant_id=participant.participantId,
            session=self.session
        )

        self.assertEqual([primary_response.authored, reconsent.authored], consent_responses[ConsentType.PRIMARY])
        self.assertEqual([ehr_response.authored], consent_responses[ConsentType.EHR])

    def test_etm_files_are_skipped(self):
        """Consent PDFs for ancillary consents (such as ETM) should not be sent in the monthly sync"""
        hpo = self.data_generator.create_database_hpo(name='consent_sync_test')
        participant = self.data_generator.create_database_participant(hpoId=hpo.hpoId)
        self.data_generator.create_database_consent_file(
            participant_id=participant.participantId,
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC
        )
        self.data_generator.create_database_consent_file(
            participant_id=participant.participantId,
            type=ConsentType.ETM,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC
        )

        files_to_sync = self.consent_dao.get_files_ready_to_sync(
            org_names=[],
            hpo_names=[hpo.name]
        )
        self.assertEqual(1, len(files_to_sync))

    def assertListsMatch(self, expected_list, actual_list, id_attribute):
        self.assertEqual(len(expected_list), len(actual_list))

        actual_id_list = [getattr(actual, id_attribute) for actual in actual_list]
        for expected_summary in expected_list:
            self.assertIn(getattr(expected_summary, id_attribute), actual_id_list)

    def _init_summary_with_consent_data(self, submitted_consent_types: List[ConsentType],
                                        validated_consent_types: List[ConsentType], extra_summary_args=None):
        # Set up participant summary fields to indicate that the consent was submitted
        # (ignoring PrimaryUpdate since that's not where it's checked)
        consent_type_to_summary_field_map = {
            ConsentType.CABOR: 'consentForCABoR',
            ConsentType.EHR: 'consentForElectronicHealthRecords',
            ConsentType.GROR: 'consentForGenomicsROR'
        }
        summary_consent_fields_to_set = {}
        consent_submissions_to_set_on_summary = [
            consent_type for consent_type in submitted_consent_types if consent_type != ConsentType.PRIMARY_UPDATE
        ]
        for consent_type in consent_submissions_to_set_on_summary:
            participant_summary_field_name = consent_type_to_summary_field_map[consent_type]
            summary_consent_fields_to_set[participant_summary_field_name] = QuestionnaireStatus.SUBMITTED

        if extra_summary_args is not None:
            summary_consent_fields_to_set.update(extra_summary_args)
        summary = self.data_generator.create_database_participant_summary(**summary_consent_fields_to_set)

        if ConsentType.PRIMARY_UPDATE in submitted_consent_types:
            # PrimaryUpdate doesn't have any fields of its own,
            # but is instead detected using questionnaire response answers
            response = self.data_generator.create_database_questionnaire_response(
                questionnaireId=self.primary_update_questionnaire.questionnaireId,
                questionnaireVersion=self.primary_update_questionnaire.version,
                participantId=summary.participantId
            )
            self.data_generator.create_database_questionnaire_response_answer(
                questionnaireResponseId=response.questionnaireResponseId,
                questionId=self.primary_update_question.questionnaireQuestionId
            )

        # Create the validation records needed and link them to the summary
        for consent_type in validated_consent_types:
            self.data_generator.create_database_consent_file(
                participant_id=summary.participantId,
                type=consent_type
            )

        return summary

    def test_loading_batch_for_revalidation(self):
        # Set up some files that need correcting, setting dates for when they were last validated (or not in some cases)
        self.data_generator.create_database_consent_file(
            last_checked=datetime(2021, 3, 2),
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            type=ConsentType.PRIMARY,
            participant_id=self.data_generator.create_database_participant(participantId=1).participantId
        )
        self.data_generator.create_database_consent_file(
            last_checked=datetime(2019, 4, 17),
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            type=ConsentType.EHR,
            participant_id=self.data_generator.create_database_participant(participantId=2).participantId
        )
        self.data_generator.create_database_consent_file(
            last_checked=None,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            type=ConsentType.GROR,
            participant_id=self.data_generator.create_database_participant(participantId=3).participantId
        )
        self.data_generator.create_database_consent_file(
            last_checked=datetime(2021, 1, 9),
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            type=ConsentType.EHR,
            participant_id=self.data_generator.create_database_participant(participantId=4).participantId
        )
        self.data_generator.create_database_consent_file(
            last_checked=None,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            type=ConsentType.PRIMARY,
            participant_id=self.data_generator.create_database_participant(participantId=5).participantId
        )
        self.data_generator.create_database_consent_file(
            last_checked=datetime(2022, 3, 2),
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            type=ConsentType.PRIMARY,
            participant_id=self.data_generator.create_database_participant(participantId=6).participantId
        )

        # Make sure the dao returns any files that haven't been checked yet,
        # and then the ones that haven't been checked recently
        result = ConsentDao.get_next_revalidate_batch(limit=4, session=self.session)
        self.assertEqual([
            (3, ConsentType.GROR),      # file not yet re-checked
            (5, ConsentType.PRIMARY),   # file not yet re-checked
            (2, ConsentType.EHR),       # file that was checked the longest ago
            (4, ConsentType.EHR),
        ], list(result))

    def test_optional_sync_filter(self):
        test_hpo_name = 'no_wear_consents'
        hpo = self.data_generator.create_database_hpo(name=test_hpo_name)
        participant = self.data_generator.create_database_participant(hpoId=hpo.hpoId)
        only_file_to_sync = self.data_generator.create_database_consent_file(
            id=27,
            participant_id=participant.participantId,
            type=ConsentType.EHR,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC
        )
        self.data_generator.create_database_consent_file(
            id=31,
            participant_id=participant.participantId,
            type=ConsentType.WEAR,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC
        )

        results = self.consent_dao.get_files_ready_to_sync(
            session=self.session,
            org_names=[],
            hpo_names=[test_hpo_name],
            additional_filters={
                test_hpo_name: {
                    'exclude_types': [ConsentType.WEAR]
                }
            }
        )
        self.assertEqual([only_file_to_sync], results)
